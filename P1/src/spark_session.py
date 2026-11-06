"""Sesión Spark: reaprovecha la configuración JDBC de la Práctica 0."""
import os
import sys
from pathlib import Path

# El mismo Python para Spark y Anaconda (especialmente en Windows).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROYECTO_DIR = Path(__file__).resolve().parent.parent
JAR_MYSQL = PROYECTO_DIR / "lib" / "mysql-connector-j-26.7.0.jar"


def crear_spark_session():
    builder = (SparkSession.builder
        .appName("IBEX35")
        .master("local[4]")
        .config("spark.driver.extraClassPath", str(JAR_MYSQL))
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
