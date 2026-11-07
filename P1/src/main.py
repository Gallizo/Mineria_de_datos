"""CONTROLADOR (C): Clase 1, ejercicios en el mismo orden del enunciado."""
from config import (
    BRONZE_IBEX, BRONZE_SENTIMIENTO, CSV_SENTIMIENTO,
    FECHA_INICIO, FECHA_FIN, TICKERS_IBEX,
)
from spark_session import crear_spark_session
from ingesta import descargar_historico, descargar_ibex, leer_sentimiento, guardar_parquet
from vista import titulo_ejercicio, mostrar_dataframe, comentar


def ejecutar_clase1():
    spark = crear_spark_session()
    try:
        # EJ1a
        titulo_ejercicio("EJ1a - Histórico de BBVA")
        df_bbva = descargar_historico(spark, "BBVA.MC", FECHA_INICIO, FECHA_FIN)
        mostrar_dataframe(df_bbva)
        comentar(
            "Fecha y Ticker no admiten nulos porque identifican cada cotización. "
            "Open, High, Low, Close y Volume sí admiten nulos para conservar los "
            "huecos de Yahoo en bronze. Dividends y Stock_Splits no admiten nulos: "
            "usamos 0.0 cuando no hay dividendos ni desdoblamientos."
        )

        # EJ1b
        titulo_ejercicio("EJ1b - IBEX 35 en bronze")
        df_ibex = descargar_ibex(spark, TICKERS_IBEX, FECHA_INICIO, FECHA_FIN)
        guardar_parquet(df_ibex, BRONZE_IBEX, "overwrite")
        print("Precios guardados en:", BRONZE_IBEX)
        print("Filas descargadas:", df_ibex.count())
        print("Tickers con datos:", df_ibex.select("Ticker").distinct().count(), "de", len(TICKERS_IBEX))

        # EJ1c 
        titulo_ejercicio("EJ1c - Leer Parquet y column pruning (opcional)")
        df_leido = spark.read.parquet(BRONZE_IBEX)
        mostrar_dataframe(df_leido)
        print("Sólo columnas Ticker, Fecha y Close:")
        df_leido.select("Ticker", "Fecha", "Close").show(5, truncate=False)
        comentar(
            "Al seleccionar sólo tres columnas, Spark puede evitar leer el resto "
            "de las columnas del Parquet; se reduce la lectura de disco."
        )

        # EJ2a
        titulo_ejercicio("EJ2a - Leer sentimiento CSV")
        df_sentimiento = leer_sentimiento(spark, CSV_SENTIMIENTO)
        mostrar_dataframe(df_sentimiento)
        comentar(
            "El CSV no almacena tipos de datos. Al no usar inferSchema, "
            "Fecha, Sector y Sentimiento llegan como texto (string). "
            "La limpieza y conversión de tipos se harán en Clase 3."
        )

        # EJ2b
        titulo_ejercicio("EJ2b - Sentimiento en bronze")
        guardar_parquet(df_sentimiento, BRONZE_SENTIMIENTO, "overwrite")
        filas_iniciales = spark.read.parquet(BRONZE_SENTIMIENTO).count()
        print("Sentimiento guardado en:", BRONZE_SENTIMIENTO)
        print("Filas escritas:", filas_iniciales)

        # ==================== EJ2c (opcional) ====================
        titulo_ejercicio("EJ2c - Modo append (opcional)")
        print("Filas antes del append:", filas_iniciales)
        guardar_parquet(df_sentimiento, BRONZE_SENTIMIENTO, "append")
        filas_finales = spark.read.parquet(BRONZE_SENTIMIENTO).count()
        print("Filas después del append:", filas_finales)
        comentar(
            "Append agrega archivos/filas nuevos, aunque sus registros sean "
            "iguales a los anteriores. Por eso el total se duplica y aparecen "
            "duplicados; Parquet no los evita automáticamente."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    ejecutar_clase1()
