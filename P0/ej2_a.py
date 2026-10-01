#Ej2-a
print("Ej2-a")

#definir sesion
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Crear una SparkSession
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./ibex35_close-2024.csv")

#para que funcione el codigo primero hay q quitar el .MC del ejercicio 1b
for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)

#eliminar duplicados y filas vacías. El numero de empresas es el numero de columnas menos 1 por la fecha
fi=df.count()
df = df.dropDuplicates().dropna(how="all")
ff=df.count()
print(f"Filas eliminadas: {fi-ff}")
print(f"Numero de empresas: {len(df.columns)-1}")
