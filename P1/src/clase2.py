from delta.tables import DeltaTable
from spark_session import crear_spark_session
from config import BRONZE_IBEX, TICKERS_IBEX
from ingesta import descargar_ibex, insertar_nuevas_cotizaciones, eliminar_ticker
from datetime import date
from pyspark.sql import functions as F


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
"""
# EJ5a - Nueva columna
df_ibex = spark.read.format("delta").load(BRONZE_IBEX)
df_nuevo = df_ibex.withColumn("Close_mas5", F.col("Close") + 5)
df_nuevo.write.format("delta").mode("append").save(BRONZE_IBEX)
"""
"""
Al ejecutarlo se ha producido un error AnalysisException por incompatibilidad de esquema,
esto es porque Delta Lake comprueba que los datos que se añaden sean compatibles, y por 
defecto no deja añadir nuevas columnas automáticamente
"""
"""
# EJ5b - Cambiar option
df_ibex = spark.read.format("delta").load(BRONZE_IBEX)
df_nuevo = df_ibex.withColumn("Close_mas5", F.col("Close") + 5)
df_nuevo.write.format("delta").mode("append").option("mergeSchema", "true").save(BRONZE_IBEX)
"""
"""
Delta Lake no ha rechazado la operación, ya que esta opción permite añadir nuevas columnas
al esquema de la tabla
"""
"""
# EJ5c - eliminar filas del ticker BBVA
tabla_ibex = DeltaTable.forPath(spark, BRONZE_IBEX)
print("BBVA antes del DELETE:")
bbva_antes = tabla_ibex.toDF().filter("Ticker = 'BBVA.MC'")
bbva_antes.show(5)
print("Número de filas:", bbva_antes.count())

eliminar_ticker(tabla_ibex, "BBVA.MC")

print("BBVA después del DELETE:")
bbva_despues = spark.read.format("delta").load(BRONZE_IBEX).filter("Ticker = 'BBVA.MC'")
bbva_despues.show(5)
print("Número de filas:", bbva_despues.count())
"""
"""
# EJ5d - Historial
tabla_ibex = DeltaTable.forPath(spark, BRONZE_IBEX)
tabla_ibex.history().show()
df_anterior = spark.read.format("delta").option("versionAsOf", 1).load(BRONZE_IBEX)
print("Filas de la versión 1:", df_anterior.count())
df_anterior.filter("Ticker = 'BBVA.MC'").show(5)
df_anterior.printSchema()
"""




spark.stop()
