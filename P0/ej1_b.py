#Ej1-b
print("Ej1-b")

#definir sesion
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Crear una SparkSession
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./ibex35_close-2024.csv")

#cambiar el nombre de las columnas y mostrar los 6 primeros valores (6 primeras filas)
for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)
df.show(6)