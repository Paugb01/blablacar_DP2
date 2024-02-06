import streamlit as st
import pandas as pd
import json

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
