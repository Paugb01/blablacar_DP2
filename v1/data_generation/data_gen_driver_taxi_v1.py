# This script:
# 1. Parses a KML file and returns a list of tuples.
# 2. Generates drivers and passengers
# 3. Sends drivers and passengers messages (JSON) to their respective PubSub topics.

import xml.etree.ElementTree as ET
import pandas as pd
import random
import string
from google.cloud import pubsub_v1
import argparse
import logging
#import secrets
import json
import time
import os
import threading
from google.cloud import bigquery

# Función para insertar cada conductor creado en BigQuery
def insert_driver_to_bigquery(driver, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Construye una nueva fila con los datos
    row_to_insert = [{
        "plate_id": driver['plate_id'],
        "seats": driver['seats'],
        "passengers": driver.get('passengers', 0)  # Inicializamos a 0
    }]

    # Inserta la entrada en BQ
    errors = client.insert_rows_json(table_id, row_to_insert)
    if errors == []:
        logging.info(f"Driver insertado en BQ: {driver['plate_id']}")
    else:
        logging.error(f"Error insertando en BQ: {errors}")

def archivo_aleatorio(directorio):
    
    archivos = os.listdir(directorio)# Obtener la lista de archivos en el directorio
    archivos_kml = [archivo for archivo in archivos if archivo.endswith('.kml')]  # Filtrar los archivos KML
    if not archivos_kml:
        raise FileNotFoundError("No se encontraron archivos KML en el directorio especificado.")
    archivo_seleccionado = random.choice(archivos_kml)  # Seleccionar aleatoriamente un archivo KML
    ruta_archivo = os.path.join(directorio, archivo_seleccionado)
    with open(ruta_archivo, 'r') as archivo:
        contenido = archivo.read()
    return ruta_archivo, contenido



# Funciones para parsear KMLs y generar datos (courses, drivers, passengers)

def course_points(kml_file):  # This function parses the KML file and returns a DF with the course.
    # Specify the path to your KML file
    kml_file_path = kml_file

    # Parse the KML file
    tree = ET.parse(kml_file_path)
    root = tree.getroot()
    
    # Extract coordinates from the correct Placemark entry. 
    # WATCH OUT HERE WITH THE Placemark")[0]. It should be 0 for all KML files but it may vary depending on the export.
    # To debug:
    # print(root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0])
    # FUTURE WORK: look in the KML file the Placermark where the course is.
    coordinates_str = root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(
        ".//{http://www.opengis.net/kml/2.2}coordinates").text.strip()

    # Separamos string en puntos individuales
    coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]

    # DF para el Placemark
    course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude'])

    # Eliminamos 'Altitude' del DF
    course_df = course_df.drop('Altitude', axis=1)

    # Tuplas con los puntos de la ruta
    course = tuple(zip(course_df['Longitude'], course_df['Latitude']))
    return course


def create_driver():
    driver = {}
    driver['plate_id'] = ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper()
    #driver['course'] = course_points(kml_file)
    driver['seats'] = int(random.uniform(4, 6))
    #driver['passengers'] = 0
    #driver['trip_cost'] = 5.0
    driver['full_tariff'] = 5.0
    driver['location'] = tuple()
    return driver

def gen_drivers(n_drivers, course):
    # Genera drivers
    drivers_list = [create_driver() for _ in range(n_drivers)]
    
    # Driver
    # Instancia de PubSub
    pubsub_class = PubSubMessages(args.project_id, args.topic_driver_name)
    for driver in drivers_list:
        insert_driver_to_bigquery(driver, 'involuted-river-411314', 'dp2', 'drivers')
        for location in course:
            driver['location'] = location
            logging.info(f"Publicando mensaje del driver: {driver['plate_id']} en la ubicación {driver['location']}")
            pubsub_class.publish_messages_driver(driver)
            time.sleep(random.uniform(1, 8))

def run_gen_drivers():
    while True:
        directorio_principal = '../Rutas'
        ruta_archivo, contenido_archivo = archivo_aleatorio(directorio_principal)
        print(ruta_archivo)
        course = course_points(ruta_archivo)
        gen_drivers(1, course)

class PubSubMessages:
    """
    Publica mensajes en el topic de Pub/Sub.
    """
    def __init__(self, project_id, topic_driver_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_driver_name = topic_driver_name
        self.topic_driver_path = self.publisher.topic_path(project_id, topic_driver_name)
        
    def publish_messages_driver(self, message):
        json_str = json.dumps(message)
        future = self.publisher.publish(self.topic_driver_path, json_str.encode("utf-8"))
        future.result()  # Espera a la publicación
        logging.info(f"Vehículo monitoreado. Id: {message['plate_id']}")

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
        # Main code

    # Input arguments
    parser = argparse.ArgumentParser(description=('Vehicle Data Generator'))
    parser.add_argument('--project_id', required=True, help='GCP cloud project name.')
    parser.add_argument('--topic_driver_name', required=True, help='PubSub_driver topic name.')
    args, opts = parser.parse_known_args()
 
        
    threads = []
        
    for _ in range(8):
        thread = threading.Thread(target=run_gen_drivers)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()    