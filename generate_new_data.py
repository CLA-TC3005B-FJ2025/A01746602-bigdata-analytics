#nombre, nickname, país, comida favorita y número de la suerte
# función aleatoria para la siguiente lista en python:
#comidas_favoritas = ["pizza", "pasta", "sushi", "tacos", "hamburguesa", "ensalada", "pollo", "chocolate", "helado", "fruta"]
import csv
import random
import faker

def generar_datos(num_registros, archivo_salida):
    """
    Genera un archivo CSV con datos de ejemplo.
    """
    fake = faker.Faker()

    with open(archivo_salida, mode='w', newline='') as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["nombre", "nickname", "país", "comida_favorita", "número_de_la_suerte"])
        comidas_favoritas = ["pizza", "pasta", "sushi", "tacos", "hamburguesa", "ensalada", "pollo", "chocolate", "helado", "fruta"]
        for _ in range(num_registros):
            nombre = fake.first_name()
            nickname = fake.user_name()
            pais = fake.country()
            comida_favorita = random.choice(comidas_favoritas)
            numero_de_la_suerte = random.randint(1, 100)
            writer.writerow([nombre, nickname, pais, comida_favorita, numero_de_la_suerte])


if __name__ == "__main__":
    numero_registros = 100000
    archivo_salida = 'nuevos_datos.csv'
    generar_datos(numero_registros, archivo_salida)
    print(f"Archivo {archivo_salida} generado con {numero_registros} registros.")