
import logging
import logging.config
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)

def build_postgres_jdbc(cfg):
    url = f"jdbc:postgresql://{cfg['HOST']}:{cfg['PORT']}/{cfg['NAME']}"
    return {"url": url, "user": cfg['USER'], "password": cfg['PASSWORD']}


class DatabaseConnector:
    

    def __init__(self, spark, rdbms: str, host: str, port: str, db_name: str, user: str, password: str):
 
        self._logger = logging.getLogger(self.__class__.__name__)
        self._spark = spark
        self._rdbms = rdbms.lower() if rdbms else "postgresql"
        self._host = host or ""
        self._port = port or ""
        self._db_name = db_name or ""
        self._user = user or ""
        self._password = password or ""

        if "postgres" in self._rdbms:
            self._default_driver = "org.postgresql.Driver"

            self._url = f"jdbc:postgresql://{self._host}:{self._port}/{self._db_name}"
        elif "mysql" in self._rdbms:
            self._default_driver = "com.mysql.cj.jdbc.Driver"
            self._url = f"jdbc:mysql://{self._host}:{self._port}/{self._db_name}"
        else:
            self._default_driver = None
            self._url = ""

        self._logger.info("DatabaseConnector init url=%s driver=%s", self._url, self._default_driver)

    def write_df_to_db(self, data_frame, db_table: str, memory_partition: int = None, write_mode: str = "overwrite", **kwargs):

        try:
            self._logger.info("Start writing dataframe to DB table=%s", db_table)

            if memory_partition is not None:
                current_partitions = data_frame.rdd.getNumPartitions()
                if current_partitions > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current_partitions < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)

            options = dict(kwargs)

            if "url" not in options:
                options["url"] = self._url

            if "user" not in options:
                options["user"] = self._user
            if "password" not in options:
                options["password"] = self._password

            options["dbtable"] = db_table

            if "driver" not in options and self._default_driver:
                options["driver"] = self._default_driver

            self._logger.debug("JDBC options used for write: %s", options)

            (data_frame.write
                .format("jdbc")
                .mode(write_mode)
                .options(**options)
                .save()
            )

        except Exception as exc:
            self._logger.error("Error writing DataFrame to DB table=%s: %s", db_table, exc, exc_info=True)
            raise
        else:
            self._logger.info("Successfully wrote DataFrame to DB table=%s", db_table)

    def read_table_to_df(self, db_table: str, **kwargs):

        try:
            self._logger.info("Start reading from database.")
            self._logger.info("---------------------- %s", self._url)
            jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}?options=-c%20TimeZone%3DAsia%2FKolkata"
            data_frame = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", db_table)             # or use .option("query", table_query)
                .option("user", self.user)
                .option("password", self.password)
                .option("driver", "org.postgresql.Driver")
                .option("options", "-c TimeZone=Asia/Kolkata") 
                .load()
            )
        except Exception as exp:
            self._logger.error(
                "Error when reading database table. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Reading database table to dataframe successfully.")
            return data_frame
        













