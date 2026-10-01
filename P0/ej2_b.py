#Ej2-b
print("Ej2-b")

#definir sesion
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Crear una SparkSession
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./ibex35_close-2024.csv")


#datos
periodo = df.select(
    min(col("Fecha")).alias("fecha_inicial"),
    max(col("Fecha")).alias("fecha_final"),
    countDistinct(col("Fecha")).alias("total_dias"),
).first()

fi = periodo["fecha_inicial"]
ff = periodo["fecha_final"]
total = periodo["total_dias"]

print(f"Periodo temporal: desde {fi} hasta {ff}")
print(f"Total de dias con informacion disponible: {total}")

#comentar
print("Es coherente porque se trabaja de lunes a viernes y hay días festivos, trabajando aproximadamente en total alrededor de 255 días al año")
print("No, ya que solo con ver los días que se ha trabajado es suficiente, si no se podrían distorsionar la información")