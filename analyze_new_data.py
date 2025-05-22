# escribe el código para crear una SparkSession, cargar los datos desde nuevos_datos.csv y realizar algunas operaciones

from pyspark.sql import SparkSession

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder.appName("Analisis de Nuevos Datos").getOrCreate()
    return spark

def cargar_datos(spark, file_path):
    """
    Carga los datos desde un archivo CSV en un DataFrame de Spark.
    """
    df = spark.read.option("header", "true").csv(file_path)
    print("Datos cargados exitosamente:")
    df.show()
    return df

#Contar el número de registros por nickname
def contar_registros_por_nickname(df):
    """
    Cuenta el número de registros por nickname y muestra los primeros 15.
    """
    print("\nNúmero de registros por nickname:")
    df.groupBy("nickname").count().show(15)
#Encontrar la comida favorita más común
def comida_favorita_mas_comun(df):
    """
    Encuentra la comida favorita más común.
    """
    print("\nComida favorita más común:")
    df.groupBy("comida_favorita").count().orderBy("count", ascending=False).show(1)
#Calcular el promedio del número de la suerte por país
def promedio_numero_suerte_por_pais(df):
    """
    Calcula el promedio del número de la suerte por país.
    """
    print("\nPromedio del número de la suerte por país:")
    df.groupBy("país").agg({"número_de_la_suerte": "avg"}).show()
#Encontrar el nickname más común
def nickname_mas_comun(df):
    """
    Encuentra el nickname más común.
    """
    print("\nNickname más común:")
    df.groupBy("nickname").count().orderBy("count", ascending=False).show(1)



def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'nuevos_datos.csv'

    # Cargar los datos
    df = cargar_datos(spark, file_path)

    # Contar el número total de registros
    total_registros = df.count()
    print(f"Número total de registros: {total_registros}")

    # Realizar más análisis
    # b. Contar el número de registros por país, mostrar los primeros 15
    print("\nNúmero de registros por país:")
    df.groupBy("país").count().show(15)

    # c. Encontrar el valor máximo y mínimo del número de la suerte
    from pyspark.sql.functions import max, min
    max_numero_suerte = df.select(max("número_de_la_suerte")).first()[0]
    min_numero_suerte = df.select(min("número_de_la_suerte")).first()[0]
    print(f"Máximo número de la suerte: {max_numero_suerte}")
    print(f"Mínimo número de la suerte: {min_numero_suerte}")

    # d. Filtrar y mostrar los registros donde la comida favorita es 'pizza'
    print("\nRegistros donde la comida favorita es 'pizza':")
    df.filter(df.comida_favorita == 'pizza').show()

    # e. Ordenar y mostrar los registros por el número de la suerte en orden descendente
    print("\nRegistros ordenados por número de la suerte (descendente):")
    df.orderBy(df.número_de_la_suerte.desc()).show()

    contar_registros_por_nickname(df)
    comida_favorita_mas_comun(df)
    promedio_numero_suerte_por_pais(df)
    nickname_mas_comun(df)




    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()