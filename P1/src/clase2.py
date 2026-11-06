from delta.tables import DeltaTable
from spark_session import crear_spark_session
from config import BRONZE_IBEX, BRONZE_SENTIMIENTO
from ingesta import convertir_a_delta

spark = crear_spark_session()

# EJ3 - Convertir Parquet a Delta
convertir_a_delta(spark, BRONZE_IBEX)
convertir_a_delta(spark, BRONZE_SENTIMIENTO)

# Mostrar historial
print("Historial IBEX:")
DeltaTable.forPath(spark, BRONZE_IBEX).history().show()
print("Historial Sentimiento:")
DeltaTable.forPath(spark, BRONZE_SENTIMIENTO).history().show()

spark.stop()

"""
Dentro de las carpetas han aparecido un archivo json que registra la primera versión de la tabla.
Al mostrar el historial con history() aparece la operación CONVERT, que confirma que ambas carpetas
se han convertido a Delta. Se han procesado 561 archivos para IBEX y 2 para sentimiento.

También han aparecido archivos de checkpoint que guardan información sobre el estado de las tablas
y archivos .crc para comprobar su integridad
"""

