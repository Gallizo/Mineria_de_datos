#Ej2-a - Realizado con la asistencia de Claude para corregir el numero de empresas
print("Ej2-a")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")


for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)

fi=df.count()
df = df.dropDuplicates()
columnas_empresas = []
for c in df.columns:
    if c != "Fecha":
        columnas_empresas.append(c)

df = df.dropna(how="all", subset=columnas_empresas)
ff=df.count()
print(f"Filas eliminadas: {fi-ff}")

conteos = df.select([count(col(c)).alias(c) for c in columnas_empresas]).first()
columnas_con_datos = [c for c in columnas_empresas if conteos[c] > 0]
df = df.select("Fecha", *columnas_con_datos)
print(f"Numero de empresas: {len(columnas_con_datos)}")
