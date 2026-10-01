#Ej2-b
print("Ej2-b")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from spark_session import spark_session
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")


df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
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

print("Es coherente porque se trabaja de lunes a viernes y hay días festivos, trabajando aproximadamente en total alrededor de 255 días al año")
print("No, ya que solo con ver los días que se ha trabajado es suficiente, si no se podrían distorsionar la información")