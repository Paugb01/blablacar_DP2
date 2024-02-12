import os
import json
from google.cloud import pubsub_v1
import streamlit as st
import streamlit_folium as st_folium
import folium

project_id = "involuted-river-411314"
subscription_name = "streamlit_visual"
bq_dataset = "dp2"
bq_table = "driver"
bucket_name = "dataflow-staging-us-central1-1069963786536"
subscription = f'projects/{project_id}/subscriptions/{subscription_name}'

# Set up Google Cloud credentials (replace 'path/to/service/account/key.json' with your actual key file path)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/service/account/key.json"

st.title("Live Map Tracking from Google Cloud Pub/Sub")

# Initialize the map centered at Valencia, Spain
valencia_coords = (39.4699, -0.3763)
m = folium.Map(location=valencia_coords, zoom_start=12)
marker = folium.Marker((0, 0), popup="Driver Location")
marker.add_to(m)
folium_static = st_folium.folium_static(m)  # Display the initial map

# Display the initial map
st.write(m)

# Google Cloud Pub/Sub Configuration
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Define callback function to process incoming messages
def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        print("Received message:", data["location"])  # Print the received message for inspection
        location = data["location"]
        longitude_driver = location[0]
        latitude_driver = location[1]

        # Initialize a new map centered at the updated driver location
        m = folium.Map(location=[longitude_driver, latitude_driver], zoom_start=12)

        # Update marker location
        marker.location = (longitude_driver, latitude_driver)

       # Clear previous map and display the updated map
        st.empty()
        m = folium.Map(location=valencia_coords, zoom_start=12)
        marker.add_to(m)
        folium_static = st_folium.folium_static(m)

    except Exception as e:
        print(f"Error processing message: {e}")

    message.ack()  

# Subscribe to Pub/Sub topic
subscriber.subscribe(subscription_path, callback=callback)

try:
    # Keep the program running to continuously receive messages
    st.write("Waiting for messages...")
    st.write("Close this window to stop receiving messages.")
    while True:
        pass
except KeyboardInterrupt:
    pass
finally:
    subscriber.close()
