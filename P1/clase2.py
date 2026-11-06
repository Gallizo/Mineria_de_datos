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



