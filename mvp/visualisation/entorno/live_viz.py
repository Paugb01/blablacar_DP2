import streamlit as st
import pandas as pd
import time

data_geo = [
    {'lon': -0.37767982482910156, 'lat': 39.46990895819994},
    {'lon': -0.37079811096191406, 'lat': 39.46675319157294},
    {'lon': -0.36390209197998047, 'lat': 39.46782397950909},
    {'lon': -0.35823822021484375, 'lat': 39.47104868526953},
    {'lon': -0.35434722900390625, 'lat': 39.47469008716329},
    {'lon': -0.3479957580566406, 'lat': 39.47431614314206},
    {'lon': -0.3437614440917969, 'lat': 39.47023227211741},
    {'lon': -0.3411865234375, 'lat': 39.46461214647102},
    {'lon': -0.3378105163574219, 'lat': 39.45855541610099},
    {'lon': -0.3328895568847656, 'lat': 39.45606173209037},
    {'lon': -0.32527923583984375, 'lat': 39.45531511819912},
    {'lon': -0.3192710876464844, 'lat': 39.45780890513396},
    {'lon': -0.3134346008300781, 'lat': 39.46104363536845},
    {'lon': -0.3088569641113281, 'lat': 39.46626045135869}
]

# def fetch_data_from_json():
#     try:
#         gdf = pd.DataFrame(data_geo)
#         return gdf
#     except Exception as e:
#         st.error(f"Error creating DataFrame: {e}")
#         return None

# def main():
#     st.title("Streamlit App for GeoJSON Data")
    
#     data = fetch_data_from_json()

#     if data is not None:
#         map_placeholder = st.empty()

#         for i in range(len(data)):
#             entry = data.iloc[i:i+1]
#             lon, lat = entry['lon'].values[i], entry['lat'].values[i]

#             # Clear the old map
#             map_placeholder.empty()

#             # Create a new map
#             map_placeholder.map(lon, lat)
#             time.sleep(2)

#         # Display additional information if needed
#         st.write("Data Details:")
#         st.write(data)

# if __name__ == "__main__":
#     main()

st.title("Streamlit App for GeoJSON Data")
data = pd.DataFrame(data_geo)
i = 0

if data is not None:
    map_var = st.empty()
map_var.empty()

lon = (-0.3088569641113281)
lat = (39.46626045135869)

map_var.map(longitude=lon, latitude=lat)
# for _ in range(0,len(data)):

#     # We clear old map 
#     map_var.empty()

#     lon = (data["lon"][i])
#     lat = (data["lat"][i])

#     map_var.map(longitude=lon, latitude=lat)
    
#     i += 1

#     time.sleep(2)

if not "sleep_time" in  st.session_state:
    st.session_state.sleep_time = 5

if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = True


if auto_refresh:
    time.sleep(3)
    st.experimental_rerun()
    

        