from spark_session import crear_spark_session
from config import BRONZE_SENTIMIENTO, SILVER_SENTIMIENTO, BRONZE_IBEX, SILVER_IBEX, BRONZE_SECTORES, SILVER_SECTORES, GOLD_IBEX_SENTIMIENTO, GOLD_PARQUET
from ingesta import obtener_info_empresa, limpiar_sentimiento, limpiar_ibex, obtener_sectores_ibex, limpiar_sectores, unir_ibex_sectores, unir_con_sentimiento, obtener_ibex_sin_sentimiento, calcular_variacion, obtener_sentimiento_sin_precios

spark = crear_spark_session()

"""
# EJ7 - Investigar información de Yahoo Finance
info = obtener_info_empresa("BBVA.MC")
print("Empresa:", info.get("longName"))
print("Sector:", info.get("sector"))
print("Industria:", info.get("industry"))
print("Capitalización:", info.get("marketCap"))


# EJ8 - Capa silver
df_bronze = spark.read.format("delta").load(BRONZE_SENTIMIENTO)
df_silver = limpiar_sentimiento(df_bronze)
df_silver.write.format("delta").mode("overwrite").save(str(SILVER_SENTIMIENTO))
print("Filas Bronze sentimiento:", df_bronze.count())
print("Filas Silver sentimiento:", df_silver.count())


# EJ9 - Limpiar datos y guardarlos en silver
df_ibex = spark.read.format("delta").load(BRONZE_IBEX)
df_silver = limpiar_ibex(df_ibex)
df_silver.write.format("delta").mode("overwrite").save(str(SILVER_IBEX))
df_silver.show(5)

df_sectores_bronze = spark.read.format("delta").load(str(BRONZE_SECTORES))
df_sectores_silver = limpiar_sectores(df_sectores_bronze)
df_sectores_silver.write.format("delta").mode("overwrite").save(str(SILVER_SECTORES))
print("Filas Silver sectores:", df_sectores_silver.count())


# EJ10 - Integrar las fuentes en Gold
df_ibex = spark.read.format("delta").load(str(SILVER_IBEX))
df_sectores = spark.read.format("delta").load(str(SILVER_SECTORES))
df_sentimiento = spark.read.format("delta").load(str(SILVER_SENTIMIENTO))

df_ibex_sectores = unir_ibex_sectores(df_ibex, df_sectores)
df_gold = unir_con_sentimiento(df_ibex_sectores, df_sentimiento)
df_gold.write.format("delta").mode("overwrite").save(str(GOLD_IBEX_SENTIMIENTO))

print("Filas Gold:", df_gold.count())
df_gold.select("Fecha", "Ticker", "Sector", "Close", "Sentimiento").show(5)



# EJ11 - Detectar registros sin sentimiento mediante anti-join
df_sin_sentimiento = obtener_ibex_sin_sentimiento(df_ibex_sectores, df_sentimiento)
print("Filas sin sentimiento:", df_sin_sentimiento.count())
df_sin_sentimiento.select("Fecha", "Ticker", "Sector", "Close").show(5)
df_sentimiento_sin_precios = obtener_sentimiento_sin_precios(df_sentimiento, df_ibex_sectores)
print("Filas de sentimiento sin precios:", df_sentimiento_sin_precios.count())
df_sentimiento_sin_precios.show(5, truncate=False)


# EJ12 - Añadir columna de variación
df_gold_variacion = calcular_variacion(df_gold)
df_gold_variacion.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(GOLD_IBEX_SENTIMIENTO))
print("Filas Gold enriquecidas:", df_gold_variacion.count())
df_gold_variacion.select("Fecha", "Ticker", "Open", "High", "Low", "Close", "Variacion_Pct", "Rango_Diario").show(5)

"""
# EJ13 - Exportar la capa Gold a Parquet
df_gold = spark.read.format("delta").load(str(GOLD_IBEX_SENTIMIENTO))
df_gold.write.format("parquet").mode("overwrite").save(str(GOLD_PARQUET))
df_parquet = spark.read.parquet(str(GOLD_PARQUET))
print("Filas exportadas a Parquet:", df_parquet.count())
df_parquet.select("Fecha", "Ticker", "Sector", "Sentimiento", "Variacion_Pct", "Rango_Diario").show(5)



spark.stop()