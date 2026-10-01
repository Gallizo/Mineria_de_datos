from db_connection import url, propiedades
#from empresas import empresas

def guardar_datos(df_original, df_final):
    df_original.write.jdbc(url, "Datos2024", "overwrite", propiedades)
    columnas = ["Dia"]
    for cod in empresas:
        columnas.append(cod)
    df_final = df_final.select(columnas)
    df_final.write.jdbc(url, "Datos2024", "overwrite", propiedades)