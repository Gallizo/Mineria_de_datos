from ingesta import obtener_info_empresa




# EJ7 - Investigar información de Yahoo Finance
info = obtener_info_empresa("BBVA.MC")
print("Empresa:", info.get("longName"))
print("Sector:", info.get("sector"))
print("Industria:", info.get("industry"))
print("Capitalización:", info.get("marketCap"))