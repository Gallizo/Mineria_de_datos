#Ej1-b
print("Ej1-b")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from spark_session import spark_session
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")


for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)
df.show(6)