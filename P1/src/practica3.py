from spark_session import crear_spark_session
from config import GOLD_IBEX_SENTIMIENTO
from pyspark.sql import functions as F
from pathlib import Path
import mlflow
from modelo_ml import (
    crear_pipeline_kmeans,
    evaluar_kmeans,
    buscar_mejor_k_hyperopt,
    mostrar_runs_mlflow,
    guardar_features,
    recuperar_features
)


spark = crear_spark_session()


"""Ejercicio 1: KMeans con las cotizaciones de BBVA"""
"""
K = 6
SEED = 42

# Preparar los datos del BBVA para KMeans
df_gold = spark.read.format("delta").load(str(GOLD_IBEX_SENTIMIENTO))
df_bbva = df_gold.filter(F.col("Ticker") == "BBVA.MC")
df_bbva = df_bbva.select("Fecha", "Open", "Close").dropna()
print("EJ1 - Filas del BBVA:", df_bbva.count())

# Entrenar KMeans
pipeline = crear_pipeline_kmeans(k=6, seed=42)
modelo = pipeline.fit(df_bbva)
predicciones = modelo.transform(df_bbva)
predicciones.groupBy("prediction").count().orderBy("prediction").show()

# Evaluar el modelo KMeans
silhouette = evaluar_kmeans(predicciones)
print("EJ1 - Silhouette:", silhouette)

# Registrar el entrenamiento en MLflow
ruta_bd = Path(__file__).resolve().parent.parent / "mlflow.db"
mlflow.set_tracking_uri(f"sqlite:///{ruta_bd.as_posix()}")
mlflow.set_experiment("Practica3_BBVA_KMeans")

with mlflow.start_run():
    mlflow.log_param("ticker", "BBVA.MC")
    mlflow.log_param("k", 6)
    mlflow.log_param("seed", 42)
    mlflow.log_param("features", "Open, Close")

    mlflow.log_metric("silhouette", float(silhouette))

    print("EJ1 - Registrado en MLflow")
    print("EJ1 - Run ID:", mlflow.active_run().info.run_id)
"""


"""Ejercicio 2: Búsqueda automática de clusters con Hyperopt"""
"""
mlflow.set_experiment("Practica3_BBVA_Hyperopt")
mejor_k = buscar_mejor_k_hyperopt(df_bbva)
print("EJ2 - Mejor número de clusters:", mejor_k)
"""

"""Ejercicio 3: Consultar los entrenamientos de MLflow"""
"""
print("EJ3 - Entrenamientos ordenados por Davies-Bouldin:")
mostrar_runs_mlflow("Practica3_BBVA_Hyperopt")
"""

"""Ejercicio 4: Versionado de características"""
# Recuperar los datos del BBVA
df_gold = spark.read.format("delta").load(str(GOLD_IBEX_SENTIMIENTO))
df_bbva = (df_gold.filter(F.col("Ticker") == "BBVA.MC").select("Fecha", "Open", "Close").dropna())
# Ruta donde guardaremos las features
ruta_features = Path(__file__).resolve().parent.parent / "data" / "lake" / "features_bbva"
# Guardar la versión 1
guardar_features(df_bbva, ruta_features, version=1)
# Recuperar la versión 1
features_v1 = recuperar_features(spark, ruta_features, version=1)
print("EJ4 - Filas recuperadas de la versión 1:", features_v1.count())

# Comprobar la reproducibilidad del modelo
pipeline = crear_pipeline_kmeans(k=6, seed=42)
modelo_original = pipeline.fit(df_bbva)
modelo_v1 = pipeline.fit(features_v1)
silhouette_original = evaluar_kmeans(modelo_original.transform(df_bbva))
silhouette_v1 = evaluar_kmeans(modelo_v1.transform(features_v1))

# Comprobar la reproducibilidad
print(f"EJ4 - Silhouette original: {silhouette_original:.6f}")
print(f"EJ4 - Silhouette versión 1: {silhouette_v1:.6f}")
if abs(silhouette_original - silhouette_v1) < 0.000001:
    print("EJ4 - Las métricas coinciden")
else:
    print("EJ4 - Las métricas son diferentes")
"""
El versionado permite recuperar las mismas características utilizadas en
el entrenamiento original. Al usar los mismos datos,parámetros y semilla,
podemos reproducir y comparar los resultados.
"""







spark.stop()