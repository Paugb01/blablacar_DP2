import geopandas as gpd
from shapely.geometry import Point
import xml.etree.ElementTree as ET

def parse_kml_coordinates(kml_file):
    tree = ET.parse(kml_file)
    root = tree.getroot()
    
    coordinates = []

    for placemark in root.iter('{http://www.opengis.net/kml/2.2}Placemark'):
        if placemark.find('{http://www.opengis.net/kml/2.2}LineString') is not None:
            coordinates_str = placemark.findtext('.//{http://www.opengis.net/kml/2.2}coordinates')
            if coordinates_str is not None:
                for coord in coordinates_str.split():
                    lon, lat, _ = map(float, coord.split(','))
                    coordinates.append(Point(lon, lat))

    return coordinates

def kml_to_dataframe(kml_file):
    coordinates = parse_kml_coordinates(kml_file)
    gdf = gpd.GeoDataFrame(geometry=coordinates)
    return gdf

# Ejemplo de uso:
kml_file = 'Indicaciones de C del Clariano, 11 a C dels Sants Just i Pastor, 153.kml'
dataframe_resultante = kml_to_dataframe(kml_file)
print(dataframe_resultante)
