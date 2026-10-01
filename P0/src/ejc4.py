#Ej4 - Realizado con la asistencia de Claude para arreglar el bucle que calcula la variación de cada empresa y para crear el DataFrame
print("Ej4")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from spark_session import spark_session
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("./data/ibex35_close-2024.csv")



for i in df.columns:
    nuevas=i.replace(".MC", "")
    df=df.withColumnRenamed(i, nuevas)

df_ordenado = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy")).orderBy("Fecha")

fila_inicial = df_ordenado.head(1)[0]
fila_final = df_ordenado.tail(1)[0]

columnas_empresas = []
for c in df.columns:
    if c != "Fecha":
        columnas_empresas.append(c)

var = []
for emp in columnas_empresas:
    filas_validas = df_ordenado.filter(col(emp).isNotNull())
    val_ini = filas_validas.head(1)[0][emp] if filas_validas.head(1) else None
    val_fin = filas_validas.tail(1)[0][emp] if filas_validas.head(1) else None

    if val_ini is not None and val_fin is not None:
        p_ini = float(val_ini)
        p_fin = float(val_fin)
        variacion = ((p_fin - p_ini) / p_ini) * 100

        if variacion >= 15.0:
            clasif = "Subida Fuerte"
        elif variacion > 1.0:
            clasif = "Subida"
        elif variacion >= -1.0:
            clasif = "Neutra"
        elif variacion > -15.0:
            clasif = "Bajada"
        else:
            clasif = "Bajada Fuerte"

        var.append((emp, p_ini, p_fin, variacion, clasif))

schema_resultado = ["Empresa", "Precio_Inicial", "Precio_Final", "Variación Anual", "Clasificacion"]
resultado_df = None
for (emp, p_ini, p_fin, variacion, clasif) in var:
    fila = spark_session.range(1).select(
        lit(emp).alias(schema_resultado[0]),
        lit(p_ini).alias(schema_resultado[1]),
        lit(p_fin).alias(schema_resultado[2]),
        lit(variacion).alias(schema_resultado[3]),
        lit(clasif).alias(schema_resultado[4])
    )
    resultado_df = fila if resultado_df is None else resultado_df.union(fila)

resultado_df.show(len(var), truncate=False)
