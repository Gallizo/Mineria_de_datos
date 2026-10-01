#Ej1-a
print("Ej1-a")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
spark_session = (SparkSession.builder .appName("IBEX35") .getOrCreate()) 
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")



df.printSchema()
fech = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
for c in fech.columns:
    if c != "Fecha":
        fech = fech.withColumn(c, col("`" + c + "`").cast("double"))
fech.printSchema()
fech.show(6)