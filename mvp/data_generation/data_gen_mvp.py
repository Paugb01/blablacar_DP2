# Import functions needed to run the script

import xml.etree.ElementTree as ET
import pandas as pd
import random, string

# Define functions to parse the KMLs and generate data (courses, drivers, passengers)

def course_points(kml_file): # This function parses the KML file and returns a DF with the course.
  # Specify the path to your KML file
  kml_file_path = kml_file

  # Parse the KML file
  tree = ET.parse(kml_file_path)
  root = tree.getroot()

  # Debug - Check Placemark entry
  #print(root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0])

  # Extract coordinates from 1st Placemark element
  coordinates_str = root.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(".//{http://www.opengis.net/kml/2.2}coordinates").text.strip()

  # Split coordinates string into individual values and convert to floats
  coordinates = [list(map(float, coord.split(','))) for coord in coordinates_str.split()]

  # Create a DF for the placemark
  course_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude', 'Altitude'])

  # Drop 'Altitude' from the DF
  course_df = course_df.drop('Altitude', axis = 1)

  return course_df

def send_location(course_df, driver_dict): # This function passes a course to a driver
  driver_dict['course'] = tuple(zip(course_df['Longitude'], course_df['Latitude'])) # 
  #print(driver_dict['course']) # Debug-only print
  return driver_dict

def create_driver(seats):
  driver = {}
  driver['plate_id'] = ''.join(random.choices(string.digits, k=4) + random.choices(string.ascii_letters, k=3)).upper() # Uniqueness condition not added, chances of repeating a plate_id are low.
  #driver['vehicle']
  #driver['colour']
  driver['course'] = tuple()
  driver['seats'] = seats
  driver['passengers'] = int()
  driver['trip_cost'] = float()
  driver['full_tariff'] = float()
  return driver

def create_passenger():
  passenger = {}
  passenger['passenger_id'] = ''.join(random.choices(string.digits, k=8) + random.choices(string.ascii_letters, k=1)).upper() # Uniqueness condition not added, chances of repeating a passenger_id are low.
  passenger['pick_location'] = tuple() # Can be used to trigger the pick-up/near pick-up and start the trip
  passenger['dropoff_location'] = tuple() # Can be used as a condition to trigger the drop-off/stop the trip
  passenger['distance'] = float() # Can be used to determine the cost of the trip
  return passenger

# Main code

if __name__ == "__main__":
    # Main code here...
    course_df = course_points("../Rutas/bioparc-lococlub.kml")
    driver_1 = create_driver(6)
    passenger_1 = create_passenger()
    #print(course_df.iloc[219])
    send_location(course_df, driver_1)
    #print(driver['course'][219])
    print(driver_1)
    print(passenger_1)