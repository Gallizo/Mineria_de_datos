#este archivo ha sido realizado con la asistencia de ChatGPT
from data_loader import guardar_datos


def ejecutar_practica():
    dataframes = {}

    # Cada import ejecuta el ejercicio tal y como ya estaba resuelto
    # y después guardamos sus DataFrames para poder reutilizarlos.
    import ej1_a
    dataframes["original"] = ej1_a.df
    dataframes["ej1_a"] = ej1_a.fech

    import ej1_b
    dataframes["ej1_b"] = ej1_b.df

    import ej2_a
    dataframes["ej2_a"] = ej2_a.df

    import ej2_b
    dataframes["ej2_b"] = ej2_b.df

    import ej3
    dataframes["ej3"] = ej3.df
    dataframes["ej3_metricas"] = ej3.metricas_df
    dataframes["ej3_deficiency"] = ej3.df_deficiency

    import ejc4
    dataframes["ej4"] = ejc4.resultado_df

    import ejc5
    dataframes["ej5"] = ejc5.df

    # Se guarda en SQL el CSV original y el DataFrame tratado del último ejercicio,
    # eliminando las columnas nuevas de cuartiles en data_loader.py.
    guardar_datos(dataframes["original"], dataframes["ej5"])

    return dataframes
