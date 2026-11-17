from spark_session import crear_spark_session
from config import GOLD_IBEX_SENTIMIENTO
from pyspark.sql import functions as F
from modelo_ml import crear_pipeline_kmeans, evaluar_kmeans
from pathlib import Path
import mlflow


spark = crear_spark_session()


"""Ejercicio 1: KMeans con las cotizaciones de BBVA"""
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


spark.stop()