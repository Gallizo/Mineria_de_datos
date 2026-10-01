#Ej1-a
print("Ej1-a")

#definir sesion
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Crear una SparkSession
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./ibex35_close-2024.csv")

#mostrar el esquema, cambiar a formato fecha y mostrar las 6 primeras filas
df.printSchema()
fech=df.withColumn("Fecha", col("Fecha").cast("date")) 
fech.printSchema()
fech.show(6)