#Ej4 - Realizado con la asistencia de Gemini para el cálculo de cuartiles y asignación de rangos
print("Ej4")

'''
hay q arreglar este codigo y mirar a ver si el is not None se puede eliminar para q quede mejor
tambien tengo q cambiar la descripcion del uso de ia
'''


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

df_ordenado = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy")).orderBy("Fecha")

fila_inicial = df_ordenado.head(1)[0]
fila_final = df_ordenado.tail(1)[0]

columnas_empresas = []
for c in df.columns:
    if c != "Fecha":
        columnas_empresas.append(c)

var = []
for emp in columnas_empresas:
    val_ini = fila_inicial[emp]
    val_fin = fila_final[emp]

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

schema_resultado = ["Empresa", "Precio_Inicial", "Precio_Final", "Variacion_Anual", "Clasificacion"]
resultado_df = spark_session.createDataFrame(var, schema=schema_resultado)

resultado_df.show(truncate=False)






