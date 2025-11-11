
import logging
import logging.config
from datetime import datetime
from src.common.database import DatabaseConnector
from src.common.s3 import S3Connector
import os 

from src.common.database import DatabaseConnector
import inspect

logger = logging.getLogger(__name__)

import inspect
    
def consume_data(read_data_db_connector, s3_connector, curated_bucket: str, report: str):

    try:
        logger.info("Publishing curated report '%s' from %s to read DB", report, curated_bucket)

        s3_key = report
        logger.debug("Reading parquet for report from bucket=%s key=%s", curated_bucket, s3_key)

        df = s3_connector.read_prq_to_df(bucket=curated_bucket, key=s3_key)

        if df is None:
            logger.error("No DataFrame returned when reading curated data for report=%s", report)
            raise RuntimeError(f"No data found for curated report {report} at {curated_bucket}/{s3_key}")

   
        try:
            if df.rdd.isEmpty():
                logger.error("DataFrame for %s is empty. Nothing to publish.", report)
                return
        except Exception as ex:
            logger.warning("rdd.isEmpty() check failed, falling back to count(): %s", ex)
            if df.count() == 0:
                logger.error("DataFrame for %s is empty after fallback count(). Nothing to publish.", report)
                return

        table_name = report
        logger.info("Writing DataFrame to DB table '%s'.", table_name)

        read_data_db_connector.write_df_to_db(data_frame=df, db_table=table_name)

        logger.info("Successfully published curated report '%s' to read DB table '%s'.", report, table_name)

    except Exception:
        logger.exception("Failed to publish curated report '%s' to read DB", report)
        raise
    
    
    
    
    
    
    
    
    
