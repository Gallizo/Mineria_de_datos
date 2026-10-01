#Ej3
print("Ej3")

#definir sesion
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Crear una SparkSession
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./ibex35_close-2024.csv")

for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)


#cambiar nombre y mostrar 10
df = df.withColumnRenamed("Fecha", "Dia")
df.show(10)

#info de empresas
columnas_empresas = []
for c in df.columns:
    if c != "Dia":
        columnas_empresas.append(c)

expresiones_agg = []
for c in columnas_empresas:
    expresiones_agg.append(avg(col(c)).alias(f"{c}_Media_anual"))
    expresiones_agg.append(max(col(c)).alias(f"{c}_Max_anual"))
    expresiones_agg.append(min(col(c)).alias(f"{c}_Min_anual"))

metricas_df = df.agg(*expresiones_agg)
metricas_df.show(truncate=False)


#precio de cierre de accion
col_uni = "`UNI.MC`" if "UNI.MC" in df.columns else "UNI"

df_deficiency = df.withColumn(
    "Deficiency Notice UNI",
    when(col(col_uni).cast("double") < 1.0, True).otherwise(False),
)

df_deficiency.show(100)
