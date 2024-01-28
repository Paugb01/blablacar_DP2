# Import necessary libraries
import xml.etree.ElementTree as ET
import pandas as pd
import random
import string
import psycopg2
import os
# Define functions to parse the KMLs and generate data (courses, drivers, passengers)

def course_points(kml_file): 
    kml_file_path = kml_file
    tree = ET.parse(kml_file_path)
    root = tree.getroot()
    coordinates_str = root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(".//{http://www.opengis.net/kml/2.2}coordinates").text.strip()
    coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]
    course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude'])
    course_df = course_df.drop('Altitude', axis=1)
    return course_df

def send_location(course_df, driver_dict):
    driver_dict['course'] = tuple(zip(course_df['Longitude'], course_df['Latitude']))
    return driver_dict

def create_driver(seats):
    driver = {}
    driver['plate_id'] = ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper()
    driver['course'] = tuple()
    driver['seats'] = seats
    driver['passengers'] = int()
    driver['trip_cost'] = float()
    driver['full_tariff'] = float()
    return driver

def create_passenger():
    passenger = {}
    passenger['passenger_id'] = ''.join(random.choices(string.digits, k=8) + random.choices(string.ascii_letters, k=1)).upper()
    passenger['pick_location'] = tuple()
    passenger['dropoff_location'] = tuple()
    passenger['distance'] = float()
    return passenger

# Function to insert data into PostgreSQL database
def insert_into_database(conductor_id, pasajero_id, numero_pasajeros, precio):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    
    # Insert data into the 'viajes' table
    cursor.execute("INSERT INTO public.viajes (conductor_id, pasajero_id, numero_pasajeros, precio) VALUES (%s, %s, %s, %s)",
                   (conductor_id, pasajero_id, numero_pasajeros, precio))
    
    conn.commit()
    cursor.close()
    conn.close()

# Main code

if __name__ == "__main__":
    course_df = course_points(r"C:\Users\diego\Documents\GitHub\blablacar_DP2\mvp\Rutas\malilla-upv.kml")
    driver_1 = create_driver(6)
    passenger_1 = create_passenger()
    
    # Insert driver into 'conductores' table
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("INSERT INTO public.conductores (matricula) VALUES (%s)", (driver_1['plate_id'],))
    conn.commit()
    cursor.close()
    conn.close()
    
    send_location(course_df, driver_1)
    print(driver_1)
    print(passenger_1)
    
    # Insert passenger into 'pasajeros' table
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("INSERT INTO public.pasajeros DEFAULT VALUES")
    conn.commit()
    cursor.close()
    conn.close()
    
    # Insert data into 'viajes' table
    insert_into_database(1, 1, 2, 20.5)

    print('DONE')



import os

def get_kml_file_paths(folder_path):
    kml_paths = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".kml"):
                kml_path = os.path.join(root, file)
                kml_paths.append(kml_path)

    return kml_paths

if __name__ == "__main__":
    folder_path = "Rutas"  # Reemplaza con la ruta de la carpeta que deseas examinar
    kml_files = get_kml_file_paths(folder_path)

    if kml_files:
        print("Archivos KML encontrados:")
        for kml_file in kml_files:
            print(kml_file)
    else:
        print("No se encontraron archivos KML en la carpeta.")

