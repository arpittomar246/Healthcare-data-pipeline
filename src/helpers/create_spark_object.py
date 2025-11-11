import logging
import logging.config
from pyspark.sql import SparkSession
import os



def create_spark_session(app_name="HealthcarePipeline"):
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraClassPath", "jars/postgresql-42.6.0.jar")
        .config("spark.jars", "jars/postgresql-42.6.0.jar")
        .config("spark.executor.extraClassPath", "jars/postgresql-42.6.0.jar")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.executorEnv.PYTHONUNBUFFERED", "1") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=utils/log4j2.xml")
        .getOrCreate()
    )

    return spark
