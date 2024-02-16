import xml.etree.ElementTree as ET
import pandas as pd
import random
import string
from google.cloud import pubsub_v1
import argparse
import logging
import json
import time
import os
import threading
from math import radians, cos, sin, asin, sqrt
from google.cloud import bigquery

# Función para meter el conductor en la tabla de BQ
def insert_driver_to_bigquery(driver, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    row_to_insert = [{
        "plate_id": driver['plate_id'],
        "seats": driver['seats'],
        "passengers": driver.get('passengers', 0)
    }]

    errors = client.insert_rows_json(table_id, row_to_insert)
    if errors == []:
        logging.info(f"Driver insertado en BQ: {driver['plate_id']}")
    else:
        logging.error(f"Error insertando en BQ: {errors}")

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Función para calcular distancias a partir de coordenadas (Haversine)
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r

# Función para elegir un archivo KML aleatorio
def archivo_aleatorio(directorio):
    archivos = os.listdir(directorio)
    archivos_kml = [archivo for archivo in archivos if archivo.endswith('.kml')]
    if not archivos_kml:
        raise FileNotFoundError("No se encontraron archivos KML en el directorio.")
    archivo_seleccionado = random.choice(archivos_kml)
    ruta_archivo = os.path.join(directorio, archivo_seleccionado)
    return ruta_archivo

# Función para parsear archivos KML
def course_points(kml_file):
    tree = ET.parse(kml_file)
    root = tree.getroot()
    coordinates_str = root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(".//{http://www.opengis.net/kml/2.2}coordinates").text.strip()
    coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]
    course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude']).drop('Altitude', axis=1)
    course = tuple(zip(course_df['Longitude'], course_df['Latitude']))
    return course

# Función para crear un conductor (payload)
def create_driver():
    driver = {
        'plate_id': ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper(),
        'seats': random.randint(4, 6),
        'full_tariff': 0.0,
        'ride_offer': 0.0,
        'location': ()
    }
    return driver

# Función para generar el conductor
def gen_drivers(n_drivers, course, project_id, topic_driver_name):
    drivers_list = [create_driver() for _ in range(n_drivers)]
    total_distance_km = 0.0
    if len(course) > 1:
        for i in range(len(course) - 1):
            total_distance_km += haversine(course[i][0], course[i][1], course[i+1][0], course[i+1][1])

    pubsub_class = PubSubMessages(project_id, topic_driver_name)
    for driver in drivers_list:
        insert_driver_to_bigquery(driver, 'involuted-river-411314', 'dp2', 'drivers')
        driver['full_tariff'] = total_distance_km * 1.50
        driver['ride_offer'] = round((driver['full_tariff'] / driver['seats']), 2)
        for location in course:
            driver['location'] = location
            logging.info(f"Publicando mensaje del driver: {driver['plate_id']} en la ubicación {driver['location']}")
            pubsub_class.publish_messages_driver(driver)
            time.sleep(random.uniform(1, 8))

# Función para montar el conductor a partir de la ruta
def run_gen_drivers(project_id, topic_driver_name):
    directorio_principal = '../Rutas'
    ruta_archivo = archivo_aleatorio(directorio_principal)
    logging.info(f"Procesando archivo KML: {ruta_archivo}")
    course = course_points(ruta_archivo)
    gen_drivers(1, course, project_id, topic_driver_name)

# Definimos clase de PubSubMessages
class PubSubMessages:
    def __init__(self, project_id, topic_driver_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_driver_name = topic_driver_name
        self.topic_driver_path = self.publisher.topic_path(project_id, topic_driver_name)

    def publish_messages_driver(self, message):
        json_str = json.dumps(message)
        future = self.publisher.publish(self.topic_driver_path, json_str.encode("utf-8"))
        future.result()
        logging.info(f"Vehículo monitoreado. Id: {message['plate_id']}")

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generador de Datos de Vehículos')
    parser.add_argument('--project_id', required=True, help='Nombre del proyecto de GCP.')
    parser.add_argument('--topic_driver_name', required=True, help='Nombre del topic de PubSub para conductores.')
    args = parser.parse_args()

    threads = []
    for _ in range(20): # Aquí el no. de instancias simultáneas
        thread = threading.Thread(target=run_gen_drivers, args=(args.project_id, args.topic_driver_name))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
