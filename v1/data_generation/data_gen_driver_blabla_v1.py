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
from math import radians, cos, sin, asin, sqrt  # Para los cálculos con Haversine

# Configuración inicial para logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def haversine(lon1, lat1, lon2, lat2):
    """Calcula la distancia del círculo máximo entre dos puntos en la tierra especificados en grados decimales."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radio de la Tierra en km
    return c * r

def archivo_aleatorio(directorio):
    """
    Selecciona un archivo KML aleatorio del directorio especificado.
    """
    archivos = os.listdir(directorio)
    archivos_kml = [archivo for archivo in archivos if archivo.endswith('.kml')]
    if not archivos_kml:
        raise FileNotFoundError("No se encontraron archivos KML en el directorio especificado.")
    archivo_seleccionado = random.choice(archivos_kml)
    ruta_archivo = os.path.join(directorio, archivo_seleccionado)
    return ruta_archivo

def course_points(kml_file):
    """
    Analiza el archivo KML y devuelve una lista de tuplas con los puntos del recorrido.
    """
    tree = ET.parse(kml_file)
    root = tree.getroot()
    coordinates_str = root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(".//{http://www.opengis.net/kml/2.2}coordinates").text.strip()
    coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]
    course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude'])
    course_df = course_df.drop('Altitude', axis=1)
    course = tuple(zip(course_df['Longitude'], course_df['Latitude']))
    return course

def create_driver():
    """
    Crea un conductor con información aleatoria.
    """
    driver = {
        'plate_id': ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper(),
        'seats': int(random.uniform(4, 6)),
        'full_tariff': 0.0,  # Se calculará basado en el recorrido
        'ride_offer': 0.0,  # Se calculará basado en full_tariff y asientos
        'location': ()
    }
    return driver

def gen_drivers(n_drivers, course, project_id, topic_driver_name):
    """
    Genera conductores, calcula tarifas y publica los mensajes en Pub/Sub.
    """
    drivers_list = [create_driver() for _ in range(n_drivers)]
    
    # Calcular la distancia total del recorrido
    total_distance_km = 0.0
    if len(course) > 1:
        for i in range(len(course) - 1):
            total_distance_km += haversine(course[i][0], course[i][1], course[i+1][0], course[i+1][1])
    print(total_distance_km)
    
    pubsub_class = PubSubMessages(project_id, topic_driver_name)
    for driver in drivers_list:
        driver['full_tariff'] = total_distance_km * 0.50  # Asumiendo €0.50 por km como tarifa
        driver['ride_offer'] = driver['full_tariff'] / driver['seats']
        for location in course:
            driver['location'] = location
            logging.info(f"Publicando mensaje del conductor: {driver['plate_id']} en la ubicación {location}")
            pubsub_class.publish_messages_driver(driver)
            time.sleep(random.uniform(1, 8))

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
    parser = argparse.ArgumentParser(description='Generador de Datos de Vehículos')
    parser.add_argument('--project_id', required=True, help='Nombre del proyecto de GCP.')
    parser.add_argument('--topic_driver_name', required=True, help='Nombre del topic de PubSub para conductores.')
    args = parser.parse_args()

    # Ejecuta la generación de conductores sin usar threading para simplificar la depuración
    directorio_principal = '../Rutas'  # Asegúrate de actualizar esta ruta
    ruta_archivo = archivo_aleatorio(directorio_principal)
    print(f"Archivo KML procesado: {ruta_archivo}")
    course = course_points(ruta_archivo)
    gen_drivers(1, course, args.project_id, args.topic_driver_name)
