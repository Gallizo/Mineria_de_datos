"""VISTA (V): sólo presenta resultados y comentarios por pantalla."""


def titulo_ejercicio(nombre):
    print(f"\n{'=' * 15} {nombre} {'=' * 15}")


def mostrar_dataframe(df, numero_filas=5):
    df.printSchema()
    df.show(numero_filas, truncate=False)


def comentar(texto, referencia="Enunciado de la Práctica 1"):
    print("\nComentario/Reflexión/Investigación")
    print(texto)
    print("Referencia/s:", referencia)
