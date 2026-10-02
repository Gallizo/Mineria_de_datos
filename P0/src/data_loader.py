from db_connection import url, propiedades


def guardar_datos(df_original, df_final):
    # Guarda los datos tal y como se leen del CSV.
    df_original.write.jdbc(url, "Datos2024", "overwrite", propiedades)

    # Ej5 añade columnas ...Cuartil. Para la tabla tratada se guardan
    # únicamente las columnas originales ya tratadas, sin columnas nuevas.
    columnas = [c for c in df_final.columns if not c.endswith("Cuartil")]
    df_final = df_final.select(columnas)
    df_final.write.jdbc(url, "Datos2024Tratados", "overwrite", propiedades)
