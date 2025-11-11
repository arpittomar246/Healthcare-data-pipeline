"""Extract data from source database and save as-is into raw zone of datalake"""
import logging
import logging.config
from datetime import datetime
from pyspark.sql import functions as func
from ..common.s3 import S3Connector
from src.common.database import DatabaseConnector
import os


logger = logging.getLogger(__name__)

def ingest_from_jdbc(spark, table_name, jdbc_cfg):

    df = spark.read.format("jdbc") \
        .option("url", jdbc_cfg["url"]) \
        .option("dbtable", table_name) \
        .option("user", jdbc_cfg.get("user")) \
        .option("password", jdbc_cfg.get("password")) \
        .load()
    return df



def ingest_data(
    source_db_connector: DatabaseConnector,
    s3_connector: S3Connector,
    raw_bucket: str,
    db_table: str,
    **kwargs,
):

    try:

        table_query = f"(SELECT * FROM {db_table}) tmp"
        logger.info("Defaulting to full-load query for table %s", db_table)

        object_num = 0
        try:
            object_num = s3_connector.check_objects_number(bucket=raw_bucket, key=db_table)
        except Exception as e:
            logger.warning("Could not check objects in datalake for %s/%s: %s. Proceeding with full load.", raw_bucket, db_table, str(e))
            object_num = 0

        if object_num and object_num > 0:
            logger.info("Found %s objects for %s in datalake. Attempting to read existing data for incremental load.", object_num, db_table)
            existing_path = os.path.join(raw_bucket, db_table).replace("\\", "/")
            existing_df = None
            try:
                existing_df = s3_connector.read_prq_to_df(path=existing_path)
            except Exception as e:
                logger.warning("Failed to read existing parquet at %s: %s. Will do full load.", existing_path, str(e))
                existing_df = None

            if existing_df is not None:
                # detect candidate id column names
                candidates = ["id", f"{db_table}_id", "drug_id", "presc_id", "prescriber_id", "presc_id", "record_id"]
                cols = [c.lower() for c in existing_df.columns]
                found = None
                for cand in candidates:
                    if cand.lower() in cols:
                        found = cand
                        break

                if found:
                    logger.info("Detected id column '%s' in existing data for table %s. Computing max to perform incremental load.", found, db_table)
                    try:
                        max_row = existing_df.agg(func.max(found).alias("max_id")).collect()[0]
                        max_val = max_row["max_id"] if "max_id" in max_row.asDict() else max_row[0]
                    except Exception as e:
                        logger.warning("Error computing max(%s) on existing data: %s. Falling back to full load.", found, str(e))
                        max_val = None

                    if max_val is not None:
                        logger.info("Will run incremental query for %s where %s > %s", db_table, found, str(max_val))
                        # Use the detected column in the WHERE clause
                        table_query = f"(SELECT * FROM {db_table} WHERE {found} > {max_val}) tmp"
                    else:
                        logger.info("No max value found; falling back to full load for %s", db_table)
                else:
                    logger.info("No candidate id column found in existing data for %s. Performing full load.", db_table)
            else:
                logger.info("Existing data not readable; performing full load for %s", db_table)
        else:
            logger.info("No existing objects found for %s; performing full load.", db_table)


        logger.info("Reading source DB with query: %s", table_query)
        ingest_df = source_db_connector.read_table_to_df(db_table=table_query, **kwargs)

        now = datetime.now()
        ingest_df = (
            ingest_df.withColumn("year", func.lit(now.year))
            .withColumn("month", func.lit(now.month))
            .withColumn("day", func.lit(now.day))
        )

        memory_partition = kwargs.get("memory_partition") or kwargs.get("memoryPartition") or kwargs.get("numPartitions")
        if memory_partition:
            try:
                memory_partition = int(memory_partition)
                cur_parts = ingest_df.rdd.getNumPartitions()
                if cur_parts > memory_partition:
                    ingest_df = ingest_df.coalesce(memory_partition)
                elif cur_parts < memory_partition:
                    ingest_df = ingest_df.repartition(memory_partition)
                logger.info("Adjusted in-memory partitions to %s for table %s", memory_partition, db_table)
            except Exception:
                logger.warning("Could not adjust memory partitions for table %s; continuing with original partitions.", db_table)

        target_path = os.path.join(raw_bucket, db_table).replace("\\", "/")
        logger.info("Writing DataFrame to %s", target_path)
        try:

            s3_connector.write_parquet(ingest_df, target_path, mode="overwrite")
        except TypeError:
  
            s3_connector.write_parquet(df=ingest_df, path=target_path, mode="overwrite")

    except Exception as exp:
        logger.error(
            "Error when ingesting data. Please check the Stack Trace. %s",
            exp,
            exc_info=True,
        )
        raise
    else:
        logger.info("Ingest table %s into S3 successfully.", db_table)
