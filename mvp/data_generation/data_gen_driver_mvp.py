# This script:
# 1. Parses a KML file and returns a list of tuples.
# 2. Generates drivers and passengers
# 3. Sends drivers and passengers messages (JSON) to their respective PubSub topics.

import xml.etree.ElementTree as ET
import pandas as pd
import random
import string
from google.cloud import pubsub_v1
#import threading
import argparse
import logging
#import secrets
import json
import time


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


def create_driver(kml_file):
    driver = {}
    driver['plate_id'] = ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper()
    driver['course'] = course_points(kml_file)
    driver['seats'] = int(random.uniform(4, 6))
    driver['passengers'] = 0
    driver['trip_cost'] = 5.0
    driver['full_tariff'] = 5.0
    driver['location'] = driver['course'][0]
    return driver

def gen_drivers(n_drivers, kml_file):
    # Generate driver
    drivers_list = []
    for driver in range(n_drivers):
        drivers_list.append(create_driver(kml_file))

    # Driver

    try:
        # Use PubSubMessages as a context manager
        pubsub_class = PubSubMessages(args.project_id, args.topic_driver_name)
        for driver in drivers_list:
            for i in range(len(driver['course'])):
                driver['location'] = driver['course'][i]
                print(driver['location'])
                # Publish driver messages
                print("Publishing driver message:", driver['plate_id']) # For debugging
                pubsub_class.publish_messages_driver(driver)
                print("Driver message published:", driver['plate_id']) # For debugging
                # Simulate randomness
                time.sleep(random.uniform(1, 10))
    except Exception as err:
        logging.error("Error while inserting data into the PubSub Topic: %s", err)

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str, topic_driver: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_driver_name = topic_driver
        self.topic_driver_path = self.publisher.topic_path(self.project_id, self.topic_driver_name)
        
    def publish_messages_driver(self, message: str):
        json_str = json.dumps(message)
        self.publisher.publish(self.topic_driver_path, json_str.encode("utf-8"))
        logging.info("A new vehicle has been monitored. Id: %s", message['plate_id'])

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
        parser.add_argument('--topic_driver_name', required=True, help='PubSub_driver topic name.')
        args, opts = parser.parse_known_args()

        # KML file path
        kml_file = "../Rutas/calle_brasil_a_mestalla.kml"
        # Generate drivers
        gen_drivers(1, kml_file)