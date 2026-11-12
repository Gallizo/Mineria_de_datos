from spark_session import crear_spark_session
from config import GOLD_IBEX_SENTIMIENTO, EXPORT_HECHOS, EXPORT_DIM_EMPRESA

spark = crear_spark_session()

# EJ1 - Preparar los datos para Power BI

# Leer la capa Gold de la Práctica 1
df_gold = spark.read.format("delta").load(str(GOLD_IBEX_SENTIMIENTO))

# Crear la tabla de hechos
df_hechos = df_gold.drop("Sector")

# Crear la dimensión de empresas
df_dim_empresa = df_gold.select("Ticker", "Sector").dropDuplicates(["Ticker"])

# Exportar ambas tablas a Parquet
df_hechos.coalesce(1).write.mode("overwrite").parquet(str(EXPORT_HECHOS))
df_dim_empresa.coalesce(1).write.mode("overwrite").parquet(str(EXPORT_DIM_EMPRESA))

# Comprobar los resultados
print("Filas hechos:", df_hechos.count())
print("Filas dimensión empresa:", df_dim_empresa.count())

df_dim_empresa.show(5)

spark.stop()