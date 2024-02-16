
import xml.etree.ElementTree as ET
import pandas as pd
import random
import string
from google.cloud import pubsub_v1
import argparse
import logging
import json
import time
import threading
from google.cloud import bigquery
import random
import os


# Función para insertar cada conductor creado en BigQuery
def insert_passenger_to_bigquery(passenger, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    location_struct = {
    "longitude": passenger['location'][1],
    "latitude": passenger['location'][0]
}
    # Construye una nueva fila con los datos
    row_to_insert = [{
        "passenger_id": passenger['passenger_id'],
        "location": location_struct,
        "ride_offer": passenger['ride_offer']  
    }]

    # Inserta la entrada en BQ
    errors = client.insert_rows_json(table_id, row_to_insert)
    if errors == []:
        logging.info(f"Driver insertado en BQ: {passenger['passenger_id']}")
    else:
        logging.error(f"Error insertando en BQ: {errors}")
# Define functions to parse the KMLs and generate data (courses, drivers, passengers)

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

    # Split coordinates string into individual values and convert to floats
    coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]

    # Create a DF for the placemark
    course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude'])

    # Drop 'Altitude' from the DF
    course_df = course_df.drop('Altitude', axis=1)

    # Returns a list of tuples
    course = tuple(zip(course_df['Longitude'], course_df['Latitude']))
    return course

def create_passenger():
    passenger = {}
    passenger['passenger_id'] = ''.join(
        random.choices(string.digits, k=8) + random.choices(string.ascii_letters, k=1)).upper()
    passenger['pick_location'] = tuple()
    passenger['location'] = tuple()
    passenger['ride_offer']= round(float(), 2)
    return passenger


def gen_passenger(n_passengers, coordinate):
    # Generate passenger
    passenger_list = []
    for passenger in range(n_passengers):
        passenger_list.append(create_passenger())
        passenger_list[passenger]['location'] = coordinate
        passenger_list[passenger]['ride_offer'] = random.uniform(1, 3)
        
    # Passengers
    duration = 200 
    try:
        # Use PubSubMessages as a context manager
        pubsub_class = PubSubMessages(args.project_id, args.topic_passenger_name)
        start_time = time.time()
        while time.time() - start_time < duration:  
            # Publish passenger messages
            for passenger in passenger_list:            
                insert_passenger_to_bigquery(passenger, 'involuted-river-411314', 'dp2', 'passengers')
                print("Publishing passenger message:", passenger['passenger_id']) # For debugging
                pubsub_class.publish_messages_passenger(passenger)
                print("Passenger message published:", passenger['passenger_id'], passenger['location']) # For debugging
            PubSubMessages(args.project_id, args.topic_passenger_name)
            # For some reason I couldn't find if we don't initialise pubsub_class after the for loop, the last message is undelivered...
    except Exception as err:
        logging.error("Error while inserting data into the PubSub Topic: %s", err)
    




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

def obtener_punto_aleatorio_desde_kml(contenido_kml):
    # Parsear el contenido KML
    root = ET.fromstring(contenido_kml)
    
    # Encontrar el elemento que contiene las coordenadas
    coordinates_element = root.find(".//{http://www.opengis.net/kml/2.2}coordinates")
    
    if coordinates_element is not None:
        # Obtener las coordenadas como una cadena
        coordenadas = coordinates_element.text.strip()
        
        # Dividir las coordenadas en una lista de puntos
        lista_puntos = coordenadas.split()
        
        # Elegir un punto aleatorio de la lista
        punto_aleatorio= random.choice(lista_puntos)
         # Convertir el punto aleatorio en una tupla
        punto_aleatorio_tupla = tuple(map(float, punto_aleatorio.split(',')))
            
        return punto_aleatorio_tupla
        
    else:
        return None

def run_gen_passengers():
    while True:
       directorio_principal = '../Rutas'
       ruta_archivo, contenido_archivo = archivo_aleatorio(directorio_principal)
       punto=obtener_punto_aleatorio_desde_kml(contenido_archivo)
       gen_passenger(1, punto)
        
        

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str, topic_passenger: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_passenger_name = topic_passenger
        self.topic_passenger_path = self.publisher.topic_path(self.project_id, self.topic_passenger_name)

    def publish_messages_passenger(self, message: str):
        json_str = json.dumps(message)
        self.publisher.publish(self.topic_passenger_path, json_str.encode("utf-8"))
        logging.info("A new passenger has been monitored. Id: %s", message['passenger_id'])

    def close(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

if __name__ == "__main__":
    # Main code

    # Input arguments
        parser = argparse.ArgumentParser(description=('Vehicle Data Generator'))
        parser.add_argument('--project_id', required=True, help='GCP cloud project name.')
        parser.add_argument('--topic_passenger_name', required=True, help='PubSub_passenger topic name.')
        args, opts = parser.parse_known_args()

       
        threads = []
        
        for _ in range(15):  
            thread = threading.Thread(target=run_gen_passengers)
            thread.start()
            threads.append(thread)
         

        for thread in threads:
            thread.join()    
