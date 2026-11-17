from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


def crear_pipeline_kmeans(k=3, seed=42):
    assembler = VectorAssembler(inputCols=["Open", "Close"], outputCol="features")
    kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=seed)
    pipeline = Pipeline(stages=[assembler, kmeans])

    return pipeline

def evaluar_kmeans(predicciones):
    evaluador = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
    return evaluador.evaluate(predicciones)