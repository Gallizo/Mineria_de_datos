from delta.tables import DeltaTable
from spark_session import crear_spark_session
from config import BRONZE_IBEX, TICKERS_IBEX
from ingesta import descargar_ibex, insertar_nuevas_cotizaciones
from datetime import date


spark = crear_spark_session()

# EJ3 - Convertir Parquet a Delta
"""
convertir_a_delta(spark, BRONZE_IBEX)
convertir_a_delta(spark, BRONZE_SENTIMIENTO)

# Mostrar historial
print("Historial IBEX:")
DeltaTable.forPath(spark, BRONZE_IBEX).history().show()
print("Historial Sentimiento:")
DeltaTable.forPath(spark, BRONZE_SENTIMIENTO).history().show()
"""

"""
Dentro de las carpetas han aparecido un archivo json que registra la primera versión de la tabla.
Al mostrar el historial con history() aparece la operación CONVERT, que confirma que ambas carpetas
se han convertido a Delta. Se han procesado 561 archivos para IBEX y 2 para sentimiento.

También han aparecido archivos de checkpoint que guardan información sobre el estado de las tablas
y archivos .crc para comprobar su integridad
"""
"""
# EJ4 - MERGE
tabla_ibex = DeltaTable.forPath(spark, BRONZE_IBEX)
version = tabla_ibex.history(1)
version.show()
df_ibex = spark.read.format("delta").load(BRONZE_IBEX)
print("Filas antes del MERGE:", df_ibex.count())


fecha_inicio = "2026-10-01"
fecha_fin = date.today().isoformat()
df_nuevos = descargar_ibex(spark, TICKERS_IBEX, fecha_inicio, fecha_fin)
print("Nuevas cotizaciones descargadas:", df_nuevos.count())

insertar_nuevas_cotizaciones(tabla_ibex, df_nuevos)
print("Filas después del primer MERGE:", spark.read.format("delta").load(BRONZE_IBEX).count())

insertar_nuevas_cotizaciones(tabla_ibex, df_nuevos)
print("Filas después del segundo MERGE:", spark.read.format("delta").load(BRONZE_IBEX).count())
"""
"""
Se descargaron 910 registros nuevos y al ejecutar el MERGE se insertaron los 910 registros nuevos.
Al ejecutar el MERGE por segunda vez con los mismos datos, el numero de filas quedó igual, ya que
evita insertar de nuevo los registros que ya existen
"""





spark.stop()
