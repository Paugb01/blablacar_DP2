import streamlit as st
import pandas as pd
import json
import streamlit_folium
from streamlit_folium import folium_static
import folium 
from folium.plugins import FloatImage
from google.oauth2 import service_account
from google.cloud import bigquery
import re


# Streamlit visualisation: set the trip you want to check. 
plate_id = '1889AFJ'


@st.cache_data(ttl=600)

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows



# Main Streamlit application
def main():
    st.title("BlaBlaCar City App")

    loc = run_query(f'''SELECT driver_id, pickup_location, dropoff_location 
                        FROM `involuted-river-411314.dp2.trips`
                        WHERE driver_id = "{plate_id}"
                        LIMIT 1''')

    point_strs = []
    pickup = loc[0]['pickup_location']
    dropoff = loc[0]['dropoff_location']
    point_strs.append(pickup)
    point_strs.append(dropoff)
    converted_points = []

    for point_str in point_strs:
        # Use regular expression to extract numeric values
        matches = re.findall(r"[-+]?\d*\.\d+|\d+", point_str)
        
        # Extract latitude and longitude values
        latitude = matches[0]
        longitude = matches[1]
        
        # Append formatted point to the list
        converted_points.append(f"{latitude}, {longitude}")

    # Splitting each string into latitude and longitude components
    split_data = [item.split(", ") for item in converted_points]

    # Creating a DataFrame
    converted_points = pd.DataFrame(split_data, columns=['lat', 'lon'])

    converted_points['lat'] = converted_points['lat'].astype(float)
    converted_points['lon'] = converted_points['lon'].astype(float)

    # Create a map centered at a specific location
    m = folium.Map(location=[converted_points['lat'].mean(), converted_points['lon'].mean()], zoom_start=14)

    # Pickup point marker (car)
    folium.Marker([converted_points.iloc[0]['lat'], converted_points.iloc[0]['lon']], 
        icon=folium.CustomIcon('./images/Black.webp', icon_size=(100, 100))).add_to(m)
    
    # Destination marker
    folium.Marker([converted_points.iloc[1]['lat'], converted_points.iloc[1]['lon']]).add_to(m)

    folium_static(m)


    # CRM Details and KPIs

    cost = run_query('''SELECT driver_id, pickup_location, dropoff_location 
                        FROM `involuted-river-411314.dp2.trips`
                        WHERE driver_id = "5254KII"
                        LIMIT 1''')

    point_strs = []
    pickup = loc[0]['pickup_location']
    dropoff = loc[0]['dropoff_location']
    point_strs.append(pickup)
    point_strs.append(dropoff)
    converted_points = []




# Create API client.
client = bigquery.Client()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.




if __name__ == "__main__":
    main()
