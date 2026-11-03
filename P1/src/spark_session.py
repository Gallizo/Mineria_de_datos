"""Sesión Spark: reaprovecha la configuración JDBC de la Práctica 0."""
import os
import sys
from pathlib import Path

# El mismo Python para Spark y Anaconda (especialmente en Windows).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


PROYECTO_DIR = Path(__file__).resolve().parent.parent
JAR_MYSQL = PROYECTO_DIR / "lib" / "mysql-connector-j-26.7.0.jar"


def crear_spark_session():
    if not JAR_MYSQL.is_file():
        raise FileNotFoundError(f"No se encuentra el conector MySQL: {JAR_MYSQL}")

    spark = (SparkSession.builder
             .appName("IBEX35")
             .master("local[*]")
             .config("spark.driver.extraClassPath", str(JAR_MYSQL))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
