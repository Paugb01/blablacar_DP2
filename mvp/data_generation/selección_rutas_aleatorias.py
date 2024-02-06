import os
import random

def archivo_aleatorio(directorio):
    # Obtener la lista de archivos en el directorio
    archivos = os.listdir(directorio)
    # Filtrar los archivos
    archivos = [archivo for archivo in archivos if os.path.isfile(os.path.join(directorio, archivo))]
    # Seleccionar un archivo aleatorio de la lista
    archivo_seleccionado = random.choice(archivos)
    # Leer el contenido del archivo seleccionado
    with open(os.path.join(directorio, archivo_seleccionado), 'r') as archivo:
        contenido = archivo.read()
    
    return contenido

# Ruta del directorio que contiene las rutas
directorio_principal = '..\\Rutas'

# Obtener el contenido de la ruta seleccionada
ruta_seleccionada = archivo_aleatorio(directorio_principal)

print("El contenido de la ruta seleccionada es:")
print(ruta_seleccionada)
