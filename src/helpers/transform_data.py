
import os
import logging
from pyspark.sql import functions as F
from src.common.s3 import S3Connector
from src.common.validation import Validation
import fnmatch


logger = logging.getLogger(__name__)


class Transformation:
    def __init__(self, s3_connector=None, cleansed_bucket=None, curated_bucket=None,
                 raw_bucket=None, logger=None, spark=None):

        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self.s3_connector = s3_connector if s3_connector is not None else S3Connector()
        self.s3 = self.s3_connector  # for compatibility
        self.cleansed_bucket = cleansed_bucket
        self.curated_bucket = curated_bucket
        self.raw_bucket = raw_bucket
        self.spark = spark
        try:
            self._vali = Validation()
        except Exception:
            self._vali = None

    def _df_has_rows(self, df):

        try:
            if df is None:
                return False

            rows = df.head(1)
            return len(rows) > 0 if rows is not None else False
        except Exception as e:
            self._logger.debug("Error while checking df rows: %s", e, exc_info=True)
            return False

    def _read_cleansed_to_df(self, table_name):

        if hasattr(self.s3, "read_prq_to_df"):

            try:
                self._logger.debug("Calling read_prq_to_df -> bucket=%s key=%s", self.cleansed_bucket, table_name)
                df = self.s3.read_prq_to_df(bucket=self.cleansed_bucket, key=table_name)
                return df
            except ValueError as ve:
         
                self._logger.debug("read_prq_to_df ValueError for %s/%s : %s", self.cleansed_bucket, table_name, ve)
            except Exception as e:
                self._logger.debug("read_prq_to_df failed for %s/%s: %s", self.cleansed_bucket, table_name, e)

        fallback_path = os.path.join(self.cleansed_bucket, table_name)
        if hasattr(self.s3, "read_parquet"):
            try:
                self._logger.debug("Calling read_parquet on %s", fallback_path)
                return self.s3.read_parquet(fallback_path)
            except Exception as e:
                self._logger.debug("read_parquet fallback failed for %s: %s", fallback_path, e)
                raise

        raise RuntimeError("No available read method on s3 connector")

    def _write_curated(self, df, report_name, partition_cols=None):
        partition_cols = partition_cols or []
        if hasattr(self.s3, "write_df_to_prq"):
            try:
                return self.s3.write_df_to_prq(df, bucket=self.curated_bucket, key=report_name, partition_cols=partition_cols)
            except TypeError:
        
                return self.s3.write_df_to_prq(bucket=self.curated_bucket, key=report_name, df=df, partition_cols=partition_cols)
        else:
            raise RuntimeError("S3 connector has no write_df_to_prq")

    def drug_report(self):
 
        try:
            self._logger.info("Building drug_report")
            presc_drug = self._read_cleansed_to_df("prescriber_drug")
            if not self._df_has_rows(presc_drug):
                self._logger.warning("prescriber_drug DataFrame is empty or None — skipping drug_report generation.")
                return


            drug = self._read_cleansed_to_df("drug")
    
            presc_agg = (
                presc_drug.groupBy("presc_id", "drug_brand_name")
                .agg(
                    F.sum("total_claims").alias("total_claims"),
                    F.sum("total_drug_cost").alias("total_drug_cost"),
                )
            )

            joined = presc_agg.join(drug, on="drug_brand_name", how="left")
            final = joined.select(
                "presc_id",
                F.coalesce("drug_brand_name", "drug_brand_name").alias("drug_brand_name"),
                "total_claims",
                "total_drug_cost",
                F.col("drug_type"),
            )

  
            self._write_curated(final, "drug_report", partition_cols=["year", "month", "day"])
            self._logger.info("drug_report written to curated/drug_report")
        except Exception as e:
            self._logger.exception("Error building drug_report: %s", e)
            raise

    def prescriber_report(self):
        try:
            self._logger.info("Building prescriber_report")
            presc_drug = self._read_cleansed_to_df("prescriber_drug")
            presc = self._read_cleansed_to_df("prescriber")
            state = self._read_cleansed_to_df("state")

            presc_agg = (
                presc_drug.groupBy("presc_id")
                .agg(
                    F.sum("total_claims").alias("total_claims"),
                    F.sum("total_drug_cost").alias("drug_cost"),
                )
            )
            joined = presc_agg.join(presc, on="presc_id", how="left")
            final = joined.join(state, joined["presc_state_code"] == state["state_code"], how="left")
            presc_report = final.select(
                "presc_id",
                F.coalesce("presc_fullname", "presc_fullname").alias("presc_fullname"),
                "presc_specialty",
                F.col("state_name").alias("presc_state"),
                "total_claims",
                "drug_cost",
            )
            self._write_curated(presc_report, "prescriber_report", partition_cols=["year", "month", "day"])
            self._logger.info("prescriber_report written to curated/prescriber_report")
        except Exception as e:
            self._logger.exception("Error building prescriber_report: %s", e)
            raise
