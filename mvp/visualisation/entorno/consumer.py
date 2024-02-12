from confluent_kafka import Consumer, KafkaError
import json
import streamlit as st
import streamlit_folium as st_folium
import folium

st.title("Live Map Tracking from Kafka Stream")

# Initialize the map centered at Valencia, Spain
valencia_coords = (39.4699, -0.3763)
m = folium.Map(location=valencia_coords, zoom_start=12)
marker = folium.Marker((0, 0), popup="Driver Location")
marker.add_to(m)
folium_static = st_folium.folium_static(m)  # Display the initial map

# Kafka Consumer Configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer
consumer = Consumer(config)

# Subscribe to Topic
topic = 'points'
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages every 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("No more messages in this partition.")
            else:
                print("Error receiving message: {}".format(msg.error()))
        else:
            # Process received message
            try:
                location = json.loads(msg.value().decode('utf-8'))
                longitude_driver = location["lat"]
                latitude_driver = location["lon"]

                # Update marker location
                marker.location = (latitude_driver, longitude_driver)

                # Clear previous map and display the updated map
                st.empty()
                m = folium.Map(location=valencia_coords, zoom_start=12)
                marker.add_to(m)
                folium_static = st_folium.folium_static(m)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON message: {e}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # Close consumer on exit
