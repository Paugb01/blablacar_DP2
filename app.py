import streamlit as st
import geopandas as gpd
from shapely.geometry import Point
import xml.etree.ElementTree as ET
import kml2geojson
import json
import os

def parse_kml_coordinates(kml_file):
    tree = ET.parse(kml_file)
    root = tree.getroot()

    coordinates = []

    for placemark in root.iter('{http://www.opengis.net/kml/2.2}Placemark'):
        if placemark.find('{http://www.opengis.net/kml/2.2}LineString') is not None:
            coordinates_str = placemark.findtext('.//{http://www.opengis.net/kml/2.2}coordinates')
            if coordinates_str is not None:
                for coord in coordinates_str.split():
                    lon, lat, alt = map(float, coord.split(','))
                    coordinates.append(Point(lon, lat, alt))

    return coordinates

def kml_to_geojson(kml_file):
    geojson_data = kml2geojson.convert(kml_file)
    return geojson_data

def kml_to_geodataframe(kml_file):
    coordinates = parse_kml_coordinates(kml_file)
    gdf = gpd.GeoDataFrame(geometry=coordinates)
    
    # Crear la columna 'lat' a partir de la segunda componente en las coordenadas
    gdf['lat'] = gdf['geometry'].apply(lambda point: point.y)
    
    # Eliminar la columna 'geometry' ya que 'lat' contiene la información necesaria
    gdf = gdf.drop('geometry', axis=1)
    
    return gdf

def main():
    st.title("Visualizador de Coordenadas KML")

    # Especificar la ruta del archivo KML directamente
    kml_file_path = "Indicaciones de C del Clariano, 11 a C dels Sants Just i Pastor, 153.kml"

    # Transformar a GeoJSON
    geojson_data = kml_to_geojson(kml_file_path)
    
    # Crear GeoDataFrame
    gdf = kml_to_geodataframe(kml_file_path)

    # Crear un directorio temporal para almacenar el archivo GeoJSON
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")  # Puedes cambiar "temp" a cualquier nombre de directorio que desees
    os.makedirs(temp_dir, exist_ok=True)

    # Guardar el GeoDataFrame en formato GeoJSON temporal
    temp_geojson_path = os.path.join(temp_dir, "temp.geojson")
    gdf.to_file(temp_geojson_path, driver="GeoJSON")

    # Mostrar el mapa especificando las columnas de latitud y longitud
    st.map(data=gdf, lat_column='lat', lon_column='longitude')

if __name__ == "__main__":
    main()


