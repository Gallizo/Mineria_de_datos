"""Rutas y datos fijos de la Clase 1."""
from pathlib import Path

# Las rutas funcionan aunque se ejecute Python desde otra carpeta.
PROYECTO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROYECTO_DIR / "data"
BRONZE_IBEX = str(DATA_DIR / "lake" / "bronze" / "ibex")
BRONZE_SENTIMIENTO = str(DATA_DIR / "lake" / "bronze" / "sentimiento")
CSV_SENTIMIENTO = str(DATA_DIR / "sentimiento_ibex.csv")

FECHA_INICIO = "2024-10-01"
FECHA_FIN = "2026-10-01"  # Último día incluido en el histórico.

# Componentes del IBEX 35 en la revisión de septiembre de 2026.
# A diferencia de una lista antigua de 36 valores, no incluimos MEL.MC:
# Meliá salió del índice al entrar Puig en julio de 2024.
TICKERS_IBEX = [
    "IBE.MC", "REP.MC", "NTGY.MC", "ELE.MC", "ENG.MC", "RED.MC",
    "SAN.MC", "BBVA.MC", "CABK.MC", "BKT.MC", "SAB.MC", "UNI.MC", "MAP.MC",
    "ACS.MC", "ANA.MC", "ANE.MC", "ACX.MC", "MTS.MC", "SCYR.MC",
    "CLNX.MC", "TEF.MC", "AENA.MC", "FER.MC", "ITX.MC", "AMS.MC", "IAG.MC",
    "GRF.MC", "FDR.MC", "SLR.MC", "ROVI.MC", "LOG.MC", "IDR.MC", "PUIG.MC",
    "COL.MC", "MRL.MC",
]

SILVER_SENTIMIENTO = PROYECTO_DIR / "data" / "lake" / "silver" / "sentimiento"
