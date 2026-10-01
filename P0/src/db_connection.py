"""Conexión con la base de datos SQL (MySQL) mediante JDBC."""
import os

HOST = os.environ.get("DB_HOST", "localhost")
PUERTO = os.environ.get("DB_PORT", "3306")
BASE_DATOS = "IBEX35"

# createDatabaseIfNotExist crea la base de datos IBEX35 si no existe todavía
URL = f"jdbc:mysql://{HOST}:{PUERTO}/{BASE_DATOS}?createDatabaseIfNotExist=true"

PROPIEDADES = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": os.environ.get("DB_USER", "root"),
    # La contraseña no se escribe en el código: se lee de la variable de entorno DB_PASSWORD
    "password": os.environ.get("DB_PASSWORD", ""),
}


def guardar_tabla(df, tabla, modo="overwrite"):
    """Escribe un DataFrame como tabla en la base de datos.

    modo: 'append', 'overwrite', 'ignore' o 'errorifexists'.
    """
    df.write.jdbc(url=URL, table=tabla, mode=modo, properties=PROPIEDADES)


def leer_tabla(spark, tabla):
    """Lee una tabla de la base de datos y devuelve un DataFrame."""
    return spark.read.jdbc(url=URL, table=tabla, properties=PROPIEDADES)
