"""MODELO (M): descarga, lectura y guardado de datos."""
from datetime import datetime, timedelta
import warnings
from pyspark.sql import functions as F
import pandas as pd
import yfinance as yf
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType,
)

# EJ1a: tipo y nulabilidad de las columnas del histórico.
ESQUEMA_PRECIOS = StructType([
    StructField("Fecha", DateType(), False),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Ticker", StringType(), False),
    StructField("Dividends", DoubleType(), False),
    StructField("Stock_Splits", DoubleType(), False),
])


def _numero(valor):
    """Convierte un número de pandas a float, respetando los nulos."""
    return None if pd.isna(valor) else float(valor)


def descargar_historico(spark, ticker, inicio, fin):
    """EJ1a: histórico de UN ticker; fin está incluido."""
    # yfinance interpreta 'end' como fecha EXCLUIDA.
    fin_exclusivo = (datetime.strptime(fin, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    pdf = yf.Ticker(ticker).history(
        start=inicio, end=fin_exclusivo, interval="1d", auto_adjust=False
    )

    if pdf.empty:
        warnings.warn(f"Yahoo Finance no devolvió precios para {ticker}.")
        return spark.createDataFrame([], ESQUEMA_PRECIOS)

    # La fecha llega en el índice de pandas: la convertimos en columna.
    pdf = pdf.reset_index()
    pdf = pdf.rename(columns={pdf.columns[0]: "Fecha"})
    pdf["Fecha"] = pd.to_datetime(pdf["Fecha"]).dt.date

    filas = []
    for _, fila in pdf.iterrows():
        volumen = fila.get("Volume")
        dividendo = fila.get("Dividends", 0)
        split = fila.get("Stock Splits", 0)
        filas.append((
            fila["Fecha"],
            _numero(fila.get("Open")),
            _numero(fila.get("High")),
            _numero(fila.get("Low")),
            _numero(fila.get("Close")),
            None if pd.isna(volumen) else int(volumen),
            ticker,
            0.0 if pd.isna(dividendo) else float(dividendo),
            0.0 if pd.isna(split) else float(split),
        ))

    return spark.createDataFrame(filas, ESQUEMA_PRECIOS)


def descargar_ibex(spark, tickers, inicio, fin):
    """EJ1b: descarga varios tickers y apila sus filas con unionByName."""
    resultado = spark.createDataFrame([], ESQUEMA_PRECIOS)
    for ticker in tickers:
        datos = descargar_historico(spark, ticker, inicio, fin)
        resultado = resultado.unionByName(datos)
    return resultado


def leer_sentimiento(spark, ruta_csv):
    """EJ2a: leer CSV sin inferir tipos (todo llega como string)."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("inferSchema", False)
            .csv(ruta_csv))


def guardar_parquet(df, ruta, modo="overwrite"):
    """EJ1b/EJ2b/EJ2c: guardar Parquet, con overwrite o append."""
    df.write.mode(modo).parquet(ruta)


"""EJ3 - Convertir Parquet a Delta"""
from delta.tables import DeltaTable
def convertir_a_delta(spark, ruta):
    DeltaTable.convertToDelta(spark, f"parquet.`{ruta}`")


"""EJ4: insertar nuevas cotizaciones"""
def insertar_nuevas_cotizaciones(tabla, nuevos):
    (tabla.alias("d")
        .merge(nuevos.alias("n"), "d.Ticker = n.Ticker AND d.Fecha = n.Fecha")
        .whenNotMatchedInsertAll()
        .execute())


"""EJ5c: eliminar ticker de una empresa"""
def eliminar_ticker(tabla, ticker):
    tabla.delete(f"Ticker = '{ticker}'")


"""EJ7: Obtener información de una empresa"""
def obtener_info_empresa(ticker):
    return yf.Ticker(ticker).info


"""EJ8: Limpiar datos de sentimiento"""
def limpiar_sentimiento(df):
    df = df.withColumn("Fecha", F.to_date("Fecha"))
    df = df.withColumn("Sector", F.lower(F.trim(F.col("Sector"))))
    df = df.withColumn("Sentimiento", F.regexp_replace(F.col("Sentimiento"), ",", ".").cast("double"))
    df = df.dropna()
    df = df.dropDuplicates(["Fecha", "Sector"])
    return df


"""EJ9: Limpiar cotizaciones del IBEX"""
def limpiar_ibex(df):
    df = df.dropna(subset=["Close", "Ticker", "Fecha"])
    df = df.dropDuplicates(["Ticker", "Fecha"])
    return df


def obtener_sectores_ibex(tickers):
    sectores = []
    for ticker in tickers:
        info = obtener_info_empresa(ticker)
        sectores.append((ticker, info.get("sector")))

    return sectores


def limpiar_sectores(df):
    df = df.withColumn("Sector", F.lower(F.trim(F.col("Sector"))))
    df = df.dropna(subset=["Ticker", "Sector"])
    df = df.dropDuplicates(["Ticker"])
    return df


"""EJ10: Integración de datos"""
def unir_ibex_sectores(df_ibex, df_sectores):
    df = df_ibex.join(df_sectores, on="Ticker", how="left")
    return df


def unir_con_sentimiento(df_ibex_sectores, df_sentimiento):
    df = df_ibex_sectores.join(
        df_sentimiento,
        on=["Fecha", "Sector"],
        how="left"
    )
    return df


"""EJ11 - Detectar cotizaciones sin sentimiento"""
def obtener_ibex_sin_sentimiento(df_ibex_sectores, df_sentimiento):
    df = df_ibex_sectores.join(
        df_sentimiento,
        on=["Fecha", "Sector"],
        how="left_anti"
    )
    return df


"""EJ12: Calcular la variación porcentual de cada día"""
def calcular_variacion(df):
    df = df.withColumn("Variacion_Pct", F.round((F.col("Close") - F.col("Open")) / F.col("Open") * 100, 2))
    return df




