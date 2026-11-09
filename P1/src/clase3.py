from spark_session import crear_spark_session
from config import BRONZE_SENTIMIENTO, SILVER_SENTIMIENTO
from ingesta import obtener_info_empresa, limpiar_sentimiento

spark = crear_spark_session()


"""
# EJ7 - Investigar información de Yahoo Finance
info = obtener_info_empresa("BBVA.MC")
print("Empresa:", info.get("longName"))
print("Sector:", info.get("sector"))
print("Industria:", info.get("industry"))
print("Capitalización:", info.get("marketCap"))
"""
"""
# EJ8 - Capa silver
df_bronze = spark.read.format("delta").load(BRONZE_SENTIMIENTO)
df_silver = limpiar_sentimiento(df_bronze)
df_silver.write.format("delta").mode("overwrite").save(str(SILVER_SENTIMIENTO))
print("Filas Bronze sentimiento:", df_bronze.count())
print("Filas Silver sentimiento:", df_silver.count())
"""







spark.stop()

