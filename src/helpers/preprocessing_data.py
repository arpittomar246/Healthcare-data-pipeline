
import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import logging
from src.common.s3 import S3Connector
from src.common.validation import Validation
import fnmatch
import traceback

logger = logging.getLogger(__name__)


class Preprocessing:
    def __init__(self, s3_connector=None, s3=None, raw_bucket=None, cleansed_bucket=None, validator=None, logger=None, **kwargs):

        self.s3 = s3_connector if s3_connector is not None else s3

        self.spark = getattr(self.s3, "spark", None)

        self.raw_bucket = raw_bucket or kwargs.get("raw_bucket") or ""
        self.cleansed_bucket = cleansed_bucket or kwargs.get("cleansed_bucket") or ""

        self._vali = validator or kwargs.get("validator")

        self._logger = logger or logging.getLogger(__name__)

        if self.s3 is None:
            self._logger.warning("Preprocessing created without an S3 connector (self.s3 is None).")

    def _build_path(self, bucket: str, *parts):

        return os.path.join(bucket, *[p.strip("/\\") for p in parts])

    def _safe_read_parquet(self, path):
 
        if not path:
            raise ValueError("empty path passed to _safe_read_parquet")

        try:

            if "/" in path and not path.endswith(".parquet"):

                b, k = path.split("/", 1)
                if hasattr(self.s3, "read_prq_to_df"):
                    self._logger.debug("Trying s3.read_prq_to_df -> bucket=%s key=%s", b, k)
                    return self.s3.read_prq_to_df(bucket=b, key=k)
        except Exception as e:
            self._logger.debug("read_prq_to_df failed for %s: %s", path, e)

        if hasattr(self.s3, "read_parquet"):
            try:
                self._logger.debug("Trying s3.read_parquet -> %s", path)
                return self.s3.read_parquet(path)
            except Exception as e:
                self._logger.debug("read_parquet failed for %s: %s", path, e)
                raise

        raise RuntimeError("No suitable read method found on S3 connector")

    def _write_cleansed(self, df, table_name, partition_cols=None):
        partition_cols = partition_cols or []
        target_key = f"{table_name}"

        if hasattr(self.s3, "write_df_to_prq"):
  
            try:
                return self.s3.write_df_to_prq(df, bucket=self.cleansed_bucket, key=target_key, partition_cols=partition_cols)
            except TypeError:
        
                return self.s3.write_df_to_prq(bucket=self.cleansed_bucket, key=target_key, df=df, partition_cols=partition_cols)
        else:
            raise RuntimeError("S3 connector has no write_df_to_prq method")

    def clean_state_data(self):
 
        table = "state"
        raw_path = self._build_path(self.raw_bucket, table)
        try:
            self._logger.info("Cleaning state data from %s", raw_path)
            df = self._safe_read_parquet(raw_path)
 
            cleaned = df.dropDuplicates()
            self._write_cleansed(cleaned, table, partition_cols=["year", "month", "day"])
            self._logger.info("State data cleaned and written to cleansed/%s", table)
        except Exception as e:
            self._logger.exception("Error in clean_state_data: %s", e)
            raise

    def clean_drug_data(self):
        table = "drug"
        raw_path = self._build_path(self.raw_bucket, table)
        try:
            self._logger.info("Cleaning drug data from %s", raw_path)
            df = self._safe_read_parquet(raw_path)
            cleaned = df.dropDuplicates()
            self._write_cleansed(cleaned, table, partition_cols=["year", "month", "day"])
            self._logger.info("Drug data cleaned.")
        except Exception as e:
            self._logger.exception("Error in clean_drug_data: %s", e)
            raise

    def clean_prescriber_data(self):
        table = "prescriber"
        raw_path = self._build_path(self.raw_bucket, table)
        try:
            self._logger.info("Cleaning prescriber data from %s", raw_path)
            df = self._safe_read_parquet(raw_path)
            cleaned = df.dropDuplicates()
            self._write_cleansed(cleaned, table, partition_cols=["year", "month", "day"])
            self._logger.info("Prescriber data cleaned.")
        except Exception as e:
            self._logger.exception("Error in clean_prescriber_data: %s", e)
            raise

    def clean_prescriber_drug_data(self):
        table = "prescriber_drug"
        raw_path = self._build_path(self.raw_bucket, table)
        try:
            self._logger.info("Cleaning prescriber_drug data from %s", raw_path)
            df = self._safe_read_parquet(raw_path)
            cleaned = df.dropDuplicates()
            if "total_claims" in cleaned.columns:
                cleaned = cleaned.withColumn("total_claims", cleaned["total_claims"].cast(IntegerType()))
            self._write_cleansed(cleaned, table, partition_cols=["year", "month", "day"])
            self._logger.info("Prescriber_drug data cleaned.")
        except Exception as e:
            self._logger.exception("Error in clean_prescriber_drug_data: %s", e)
            raise
