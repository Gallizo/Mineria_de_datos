#Ej3 - Realizado con la asistencia de Claude para corregir el cálculo de máximos/mínimos y el formato por empresa
print("Ej3")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")



for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)


df = df.withColumnRenamed("Fecha", "Dia")
df = df.dropDuplicates().withColumn("Dia", to_date(col("Dia"), "dd/MM/yyyy")).orderBy("Dia")
df.show(10)

columnas_empresas = []
for c in df.columns:
    if c != "Dia":
        columnas_empresas.append(c)

n = len(columnas_empresas)
pares = ", ".join([f"'{c}', cast(`{c}` as double)" for c in columnas_empresas])
largo = df.select(expr(f"stack({n}, {pares}) as (Empresa, Precio)"))
metricas_df = largo.groupBy("Empresa").agg(
    avg("Precio").alias("Media anual"),
    max("Precio").alias("Max anual"),
    min("Precio").alias("Min anual"),
)
metricas_df.show(n, truncate=False)

col_uni = "`UNI.MC`" if "UNI.MC" in df.columns else "UNI"

df_deficiency = df.withColumn(
    "Deficiency Notice UNI",
    when(col(col_uni).cast("double") < 1.0, True).otherwise(False),
)

df_deficiency.show(100)


