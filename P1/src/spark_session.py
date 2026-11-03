"""Creación de Spark en modo local; se reutilizará en otras clases."""
import os
import sys

# Ayuda a evitar que Spark lance un Python distinto al del entorno Anaconda.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def crear_spark_session():
    spark = (SparkSession.builder
             .appName("IBEX35_Clase1")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
