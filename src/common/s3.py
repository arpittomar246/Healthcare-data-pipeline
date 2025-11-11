import os
from typing import Optional, List
from pyspark.sql import DataFrame
import glob
from typing import Optional

def _is_s3_path(path: Optional[str]) -> bool:
    if not path:
        return False
    p = str(path).lower()
    return p.startswith("s3a://") or p.startswith("s3://")

def _normalize_local_path(path: str) -> str:

    return os.path.normpath(path).replace("\\", "/").rstrip("/")

class S3Connector:
    def __init__(self, spark=None, logger=None, local_base: Optional[str] = None):

        self.spark = spark
        self._logger = logger
        self.local_base = local_base or os.getcwd()
        
        
    def read_prq_to_df(self, bucket: Optional[str] = None, key: Optional[str] = None):
    

        bucket = (bucket or "").strip().rstrip("/\\")
        key = (key or "").strip().strip("/\\")
        candidates = []

        if bucket and key:
            candidates.append(os.path.join(bucket, key).replace("\\", "/"))
        if bucket:
            candidates.append(bucket.replace("\\", "/"))
        if key:
            candidates.append(key.replace("\\", "/"))

        extra_candidates = []
        for path in list(candidates):
            parts = path.replace("\\", "/").split("/")

            for i in range(len(parts), 0, -1):
                parent = "/".join(parts[:i])
                if parent and parent not in candidates and parent not in extra_candidates:
                    extra_candidates.append(parent)
        candidates.extend(extra_candidates)

        def dir_has_parquet_files(path):
            try:

                patterns = [os.path.join(path, "part-*"), os.path.join(path, "*.parquet")]
                for pat in patterns:
                    found = [f for f in glob.glob(pat) if not os.path.basename(f).startswith(".")]
                    if any(os.path.getsize(f) > 0 for f in found):
                        return True
                return False
            except Exception:
                return False

        attempted = []
        last_exc = None
        discovered_parents = []

        for path in candidates:
            if not path:
                continue

            attempted.append(path)

            if not os.path.exists(path):
                continue


            if os.path.isdir(path) and not dir_has_parquet_files(path):

                discovered_parents.append(path)
                continue

            try:
  
                if not hasattr(self, "spark") or self.spark is None:
                    raise RuntimeError("S3Connector has no SparkSession attached (self.spark is None).")
                df = self.spark.read.parquet(path)
                return df
            except Exception as e:
                last_exc = e


        msg_lines = []
        msg_lines.append("read_prq_to_df could not read parquet. Tried these paths:")
        for p in attempted:
            msg_lines.append(p)
        if discovered_parents:
            msg_lines.append("\nDiscovered directories without part-*/.parquet files:")
            for p in discovered_parents:
                msg_lines.append(" - " + p)
        if last_exc is not None:
            msg_lines.append(f"Last error: {type(last_exc).__name__}: {str(last_exc)}")
        else:
            msg_lines.append("No parquet files (part-*/.parquet) found under candidate dirs.")

        raise RuntimeError("\n".join(msg_lines))


    def list_objects_local(self, base_folder: str, prefix: str = "") -> List[str]:

        base = base_folder if os.path.isabs(base_folder) else os.path.join(self.local_base, base_folder)
        base = _normalize_local_path(base)
        if not os.path.exists(base):
            return []
        out = []
        for root, _, files in os.walk(base):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, base).replace("\\", "/")
                if prefix:
                    if rel.startswith(prefix):
                        out.append(rel)
                else:
                    out.append(rel)
        return out

    def list_objects_s3_via_spark(self, s3_uri: str) -> Optional[List[str]]:

        if not self.spark:
            return None
        try:
            hconf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
            jp = self.spark._jvm.org.apache.hadoop.fs.Path(s3_uri)
            if not fs.exists(jp):
                return []
            status = fs.listStatus(jp)
            out = [stat.getPath().toString() for stat in status]
            return out
        except Exception:
            return None

    def list_objects_s3_via_boto3(self, bucket: str, prefix: str = "") -> Optional[List[str]]:

        try:
            import boto3
            client = boto3.client("s3")
            paginator = client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys
        except Exception:
            return None

    def check_objects_number(self, bucket: str, key: str = "") -> int:

        if not bucket:
            return 0


        if _is_s3_path(bucket):
            without = bucket.split("://", 1)[1]
            parts = without.split("/", 1)
            b = parts[0]
            base_prefix = parts[1] if len(parts) > 1 else ""
            prefix = "/".join(filter(None, [base_prefix.rstrip("/"), key.lstrip("/")])) if key else base_prefix

            keys = self.list_objects_s3_via_boto3(b, prefix)
            if keys is not None:
                return len(keys)

            s3_uri = f"s3a://{b}/{prefix}" if prefix else f"s3a://{b}"
            keys = self.list_objects_s3_via_spark(s3_uri)
            if keys is None:
  
                return 0
            return len(keys)


        base = bucket if os.path.isabs(bucket) else os.path.join(self.local_base, bucket)
        if key:
            search = os.path.join(base, key)
        else:
            search = base
        search = _normalize_local_path(search)
        if not os.path.exists(search):
            return 0
        if os.path.isfile(search):
            return 1
        count = 0
        for _, _, files in os.walk(search):
            count += len(files)
        return count


    def write_parquet(self, df: DataFrame, path: str = None, mode: str = "overwrite", bucket: str = None, key: str = None, **kwargs):

        if path is None:
            if bucket:
                path = os.path.join(bucket, key) if key else bucket
            else:
                raise ValueError("Either 'path' or 'bucket' must be provided to write_parquet().")
        path = _normalize_local_path(path)


        if _is_s3_path(path):
            df.write.mode(mode).parquet(path)
            if self._logger:
                self._logger.info(f"DataFrame written to {path} (s3)")
            else:
                print(f"DataFrame written to {path} (s3)")
            return

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)


        partition_col = kwargs.get("partitionColumn") or kwargs.get("partition_col")
        if partition_col:
            df.write.mode(mode).partitionBy(partition_col).parquet(path)
        else:
            df.write.mode(mode).parquet(path)

        if self._logger:
            self._logger.info(f"DataFrame written to {path}")
        else:
            print(f"DataFrame written to {path}")


    def write_df_to_prq(self, df: DataFrame, *args, **kwargs):
        return self.write_parquet(df, *args, **kwargs)

    def read_parquet(self, path: str):
    
        if not self.spark:
            raise ValueError("S3Connector.read_parquet requires a SparkSession (self.spark).")
        return self.spark.read.parquet(path)





