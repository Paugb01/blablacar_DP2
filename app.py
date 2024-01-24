import streamlit as st
import geopandas as gpd
from shapely.geometry import Point
import xml.etree.ElementTree as ET
import kml2geojson
import json

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
    return gdf

def main():
    st.title("Visualizador de Coordenadas KML")

    # Archivo KML
    kml_file = st.file_uploader("Cargar archivo KML", type=["kml"])

    if kml_file is not None:
        st.subheader("Mapa de Coordenadas")

        # Transformar a GeoJSON
        geojson_data = kml_to_geojson(kml_file)
        
        # Crear GeoDataFrame
        gdf = kml_to_geodataframe(kml_file)

        gdf['latitude'] = gdf['geometry'].y
        gdf['longitude'] = gdf['geometry'].x

        # Mostrar el mapa especificando las columnas de latitud y longitud
        st.map(data=gdf, lat_column='latitude', lon_column='longitude')

if __name__ == "__main__":
    main()
