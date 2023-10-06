import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

PATH_JARS = Path(__file__).absolute().parent / 'jars'


def get_spark(app_name: str = "spark_training_spark3", master: str = "local[2]") -> SparkSession:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark_conf = pyspark.SparkConf().setAppName(app_name).setMaster(master)
    spark_conf = spark_conf.set("spark.jars.packages",
                                ','.join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
                                          "io.delta:delta-core_2.12:2.4.0",
                                          "org.xerial:sqlite-jdbc:3.43.0.0"]))
    spark_conf = spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf = spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    assert (spark.sparkContext.pythonVer == "3.11")
    print(spark.sparkContext.uiWebUrl)
    return spark
