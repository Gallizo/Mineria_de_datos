"""Carga de datos: único sitio donde se define la ruta y las opciones de lectura del CSV."""
import os

# Ruta relativa a este archivo (src/ -> ../data/), funciona se ejecute desde donde se ejecute
RUTA_CSV = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "ibex35_close-2024.csv")
)


def cargar_datos(spark, ruta=RUTA_CSV):
    """Lee el CSV del IBEX-35 y devuelve el DataFrame tal cual (todo en string)."""
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("dateFormat", "dd/MM/yyyy")
        .csv(ruta)
    )
