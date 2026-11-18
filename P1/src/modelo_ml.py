from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from sklearn.metrics import davies_bouldin_score
import mlflow
import numpy as np
from hyperopt import fmin, hp, tpe, STATUS_OK
from pyspark.sql import functions as F
from delta.tables import DeltaTable


"""ej1"""
def crear_pipeline_kmeans(k=3, seed=42):
    assembler = VectorAssembler(inputCols=["Open", "Close"], outputCol="features")
    kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=seed)
    pipeline = Pipeline(stages=[assembler, kmeans])

    return pipeline

def evaluar_kmeans(predicciones):
    evaluador = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
    return evaluador.evaluate(predicciones)


"""ej2"""
def evaluar_davies_bouldin(predicciones):
    datos = predicciones.select("Open", "Close", "prediction").toPandas()
    return davies_bouldin_score(datos[["Open", "Close"]], datos["prediction"])


def buscar_mejor_k_hyperopt(df_bbva):
    def objetivo(parametros):
        k = int(parametros["k"])
        pipeline = crear_pipeline_kmeans(k=k, seed=42)
        modelo = pipeline.fit(df_bbva)
        predicciones = modelo.transform(df_bbva)
        db = evaluar_davies_bouldin(predicciones)
        with mlflow.start_run(run_name=f"EJ2_KMeans_k{k}"):
            mlflow.log_param("k", k)
            mlflow.log_param("seed_modelo", 42) # Seed=42 en KMeans: fija la inicialización del modelo
            mlflow.log_param("seed_hyperopt", 42) # Seed=42 en Hyperopt: fija la búsqueda de hiperparámetros
            mlflow.log_metric("davies_bouldin", float(db))

        print(f"EJ2 - k={k}, Davies-Bouldin={db:.4f}")

        return {"loss": float(db), "status": STATUS_OK}

    espacio = {"k": hp.quniform("k", 2, 8, 1)}

    mejor = fmin(
        fn=objetivo,
        space=espacio,
        algo=tpe.suggest,
        max_evals=8,
        points_to_evaluate=[{"k": k} for k in range(2, 7)],
        rstate=np.random.default_rng(42),
        show_progressbar=False
    )
    return int(mejor["k"])


"""ej3"""
def mostrar_runs_mlflow(nombre_experimento):
    experimento = mlflow.get_experiment_by_name(nombre_experimento)
    if experimento is None:
        print("EJ3 - Experimento no encontrado")
        return

    runs = mlflow.search_runs(experiment_ids=[experimento.experiment_id])
    if runs.empty:
        print("EJ3 - No hay entrenamientos registrados")
        return

    runs = runs.sort_values(by="metrics.davies_bouldin", ascending=True)
    for _, run in runs.iterrows():
        print(
            f"Run ID: {run['run_id']} | "
            f"k: {run['params.k']} | "
            f"Seed modelo: {run['params.seed_modelo']} | "
            f"Seed Hyperopt: {run['params.seed_hyperopt']} | "
            f"Davies-Bouldin: {run['metrics.davies_bouldin']:.4f}")



"""ej4"""
def guardar_features(df, ruta, version):
    spark = df.sparkSession
    # Evitar guardar dos veces la misma versión
    if DeltaTable.isDeltaTable(spark, str(ruta)):
        guardadas = spark.read.format("delta").load(str(ruta))
        if guardadas.filter(F.col("version") == version).limit(1).count() > 0:
            print(f"EJ4 - La versión {version} ya está guardada")
            return

    (
        df.withColumn("Ticker", F.lit("BBVA.MC"))
        .withColumn("version", F.lit(version))
        .write.format("delta")
        .mode("append")
        .save(str(ruta))
    )
    print(f"EJ4 - Versión {version} guardada correctamente")


def recuperar_features(spark, ruta, version):
    return (spark.read.format("delta").load(str(ruta)).filter(F.col("version") == version).select("Fecha", "Open", "Close"))




"""ej5"""
def comparar_experimentos():
    print("EJ5 - Experimentos disponibles:")
    for experimento in mlflow.search_experiments():
        print(f"\nExperimento: {experimento.name}")
        runs = mlflow.search_runs(experiment_ids=[experimento.experiment_id])
        if runs.empty:
            print("Sin ejecuciones")
            continue
        # Davies-Bouldin se minimiza; Silhouette se maximiza.
        if "metrics.davies_bouldin" in runs.columns:
            metrica = "metrics.davies_bouldin"
            ascendente = True
        elif "metrics.silhouette" in runs.columns:
            metrica = "metrics.silhouette"
            ascendente = False
        else:
            print("Sin métricas de evaluación")
            continue

        mejor = runs.sort_values(by=metrica, ascending=ascendente).iloc[0]
        print("Mejor Run ID:", mejor["run_id"])
        print("Clusters:", mejor.get("params.k", "N/D"))
        print(f"{metrica}: {mejor[metrica]:.4f}")

