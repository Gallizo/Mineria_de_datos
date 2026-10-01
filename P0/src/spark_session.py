"""Inicialización de Spark: único sitio donde se configura la SparkSession."""
import os
import sys

from pyspark.sql import SparkSession

# Carpeta del proyecto (P0) calculada a partir de este archivo, sin rutas absolutas
RAIZ_PROYECTO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# Ruta (relativa al proyecto) del .jar del conector MySQL. Hay que colocarlo en P0/lib/
RUTA_JAR_MYSQL = os.path.join(RAIZ_PROYECTO, "lib", "mysql-connector-j-8.4.0.jar")


def crear_spark_session(app_name="IBEX35", usar_jdbc=False):
    """Crea (o recupera) la SparkSession.

    usar_jdbc=True añade el .jar del conector MySQL al classpath del driver.
    """
    # Spark usará el mismo Python que ejecuta el script (evita el error
    # "Python worker failed to connect back" en Windows) sin rutas absolutas
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = SparkSession.builder.appName(app_name)

    if usar_jdbc:
        builder = builder.config("spark.driver.extraClassPath", RUTA_JAR_MYSQL)

    return builder.getOrCreate()
