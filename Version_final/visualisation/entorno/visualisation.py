import streamlit as st
import pandas as pd
import json
import streamlit_folium
from streamlit_folium import folium_static
import folium 
from folium.plugins import FloatImage
from google.oauth2 import service_account
from google.cloud import bigquery
import plotly.express as px
import re


# Streamlit visualisation: set the trip you want to check. 
trip_id = '35afebae-15f4-4110-a3cc-249b37321b30'

@st.cache_data(ttl=600)

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


# Main Streamlit application
def main():
    st.title(":blue[BlaBlaCar] City App")

    passenger = run_query(f'''SELECT location
                              FROM involuted-river-411314.dp2.passengers
                              WHERE passenger_id = (SELECT passenger_id
                              FROM `involuted-river-411314.dp2.trips`
                              WHERE trip_id = "{trip_id}"
                              LIMIT 1)''')
    print(passenger)
    latitude_p = passenger[0]['location']['longitude']
    longitude_p = passenger[0]['location']['latitude']

    st.header('User view demonstration :speaking_head_in_silhouette:', divider='violet')
    st.header(f'Your driver is waiting for you :sunglasses: :car:')

    loc = run_query(f'''SELECT driver_id, pickup_location, dropoff_location 
                        FROM `involuted-river-411314.dp2.trips`
                        WHERE trip_id = "{trip_id}"
                        LIMIT 1''')
    cost = run_query(f'''SELECT cost, travelled_distance
                        FROM `involuted-river-411314.dp2.trips`
                        WHERE trip_id = "{trip_id}"
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
    
    # Destination point
    folium.Marker([converted_points.iloc[1]['lat'], converted_points.iloc[1]['lon']]).add_to(m)

    # Passenger location
    folium.Marker([latitude_p, longitude_p], 
        icon=folium.CustomIcon('./images/person.png', icon_size=(70, 70))).add_to(m)
    
    folium_static(m)

    st.header(f"Your destination is {cost[0]['travelled_distance']} kms away for a price of :blue[{cost[0]['cost']}€] :racing_car: :100:")

    # CRM Details and KPIs
    st.header('Business Platform :bar_chart:', divider='blue')

    drivers = run_query(f'''SELECT COUNT(DISTINCT plate_id)
                        FROM `involuted-river-411314.dp2.drivers`''')
    
    total_volume_traded = run_query(f'''SELECT SUM(cost)
                        FROM `involuted-river-411314.dp2.trips`''')
    
    avg_income = run_query(f'''SELECT company_margin
                                    FROM `involuted-river-411314.dp2.trips`
                                    WHERE company_margin IS NOT NULL''')
    
    total_income = run_query(f'''SELECT SUM(company_margin)
                                    FROM `involuted-river-411314.dp2.trips`
                                    WHERE company_margin IS NOT NULL''')
    
    total_drivers_with_match = run_query(f'''SELECT COUNT(DISTINCT driver_id)
                                    FROM `involuted-river-411314.dp2.trips`
                                    WHERE travelled_distance <> 0.0''')
    

    
    match_ratio = round(float(total_drivers_with_match[0]['f0_'])/float(drivers[0]['f0_']),2)
    



    inc = float(total_income[0]['f0_'])
    tvt = round(total_volume_traded[0]['f0_'],2)
 
    st.subheader(f"Up to date, there are {drivers[0]['f0_']} drivers enrolled to the platform.")
    st.subheader(f"A total of {tvt}€ have been saved by drivers thanks to our platform.")


    chart_data = pd.DataFrame({'Total income': [inc],
                               'Total volume traded': [tvt]}, columns=['Total income', 'Total volume traded'])

    st.bar_chart(chart_data)

    st.header(f'Around {match_ratio*100}% of our drivers has found a match! :heart: :fire:')


    df = {'drivers with no match': drivers[0]['f0_']-total_drivers_with_match[0]['f0_'],
        'drivers with match': total_drivers_with_match[0]['f0_']}

    fig = px.pie(values=list(df.values()), names=list(df.keys()), title='Drivers Matching Status')
    st.plotly_chart(fig)


# Create API client.
client = bigquery.Client()

if __name__ == "__main__":
    main()
