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


@st.cache_data(ttl=600)
# Function to fetch data from GeoJSON file
def fetch_data_from_json():
    # Read GeoJSON data into a GeoDataFrame
    with open('../json/driver.json', 'r') as file:
        data = json.load(file)
    
    # st.write("Loaded GeoJSON data:")
    # st.write(data)
    plate_id = data["plate_id"]
    price_to_pay = data["trip_cost"]
    st.write(f'Your car trip identifier is: {plate_id}. Price to pay: {price_to_pay}€')
    use_data = data["location"][0]
    # st.write(use_data)
    try:
        gdf = pd.DataFrame(use_data, index=['coordinates'])
        return gdf
    except Exception as e:
        st.error(f"Error creating DataFrame: {e}")
        return None

# Main Streamlit application
def main():
    st.title("Streamlit App for GeoJSON Data")

    # Fetch data from GeoJSON
    data = fetch_data_from_json()

    if data is not None:
        # Display GeoDataFrame on the map
        st.map(data)

        # Display additional information if needed
        st.write("Data Details:")
        st.write(data)

if __name__ == "__main__":
    main()




# Create API client.
client = bigquery.Client()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

rows = run_query('''SELECT driver_id, pickup_location, dropoff_location 
                    FROM `involuted-river-411314.dp2.trips`
                    WHERE driver_id = "5254KII"
                    LIMIT 1''')

st.write(rows)



points = []
point_strs = []
pickup = rows[0]['pickup_location']
dropoff = rows[0]['dropoff_location']
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

st.write(converted_points)

converted_points['lat'] = converted_points['lat'].astype(float)
converted_points['lon'] = converted_points['lon'].astype(float)

# Create a map centered at a specific location
m = folium.Map(location=[converted_points['lat'].mean(), converted_points['lon'].mean()], zoom_start=14)

# Add markers with custom images
for _, row in converted_points.iterrows():
    folium.Marker([row['lat'], row['lon']], 
                  icon=folium.CustomIcon('./images/Black.webp', icon_size=(100, 100))).add_to(m)

# Display the map
folium_static(m)

