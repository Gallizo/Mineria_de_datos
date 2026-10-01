# Ej5 - Realizado con la asistencia de Gemini para el cálculo de cuartiles y asignación de rangos
print("Ej5")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")



for c in df.columns:
    df = df.withColumnRenamed(c, c.replace(".MC", ""))

if "Fecha" in df.columns:
    df = df.withColumnRenamed("Fecha", "Dia")

columnas_empresas = []
for c in df.columns:
    if c not in ["Dia", "Fecha", "Deficiency Notice UNI"]:
        columnas_empresas.append(c)

for c in columnas_empresas:
    df = df.withColumn(c, col(c).cast("double"))

df = df.dropDuplicates()
df = df.withColumn("Dia", to_date(col("Dia"), "dd/MM/yyyy")).orderBy("Dia")


for c in columnas_empresas:
    cuartiles = df.approxQuantile(c, [0.25, 0.5, 0.75], 0.01)
    if cuartiles and len(cuartiles) == 3:
        q1 = cuartiles[0]
        q2 = cuartiles[1]
        q3 = cuartiles[2]
        nombre_col_cuartil = f"{c}Cuartil"
        
        df = df.withColumn(
            nombre_col_cuartil,
            when(col(c).isNull(), lit(None).cast("string"))
            .when(col(c) <= q1, "q1")
            .when(col(c) <= q2, "q2")
            .when(col(c) <= q3, "q3")
            .otherwise("q4")
        )

df.show(1, truncate=False)

cols_aena_bbva = ["AENA", "AENACuartil", "BBVA", "BBVACuartil"]
df.select(*cols_aena_bbva).show(df.count(), truncate=False)


