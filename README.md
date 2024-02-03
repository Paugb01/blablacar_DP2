# ShortTripShare
### ShortTripShare is a platform developed by BlaBlaCar 🚙
#### Our aim is to be able to share short-distance rides within cities or between towns. This platform allows users to post their short trips from point A 📍 to point B 📍, and other users who need to travel from point A to a destination B' along the way can find and join these trips. The platform facilitates the entire process, from trip posting to transaction  completion.

-   23/01/2024:
    -   jumepe [ADDED] data_gen_mvp.py:
        -   Python script containing functions to parse course data from KML file to a dataframe, generate drivers, passengers and pass a course to a driver.

-   3/02/2024:
    -   pagaba [ADDED] mvp/visualisation:
	-   Contains a venv, producer which sends kafka messages to test streamlit live map visualisation. Consumer.py receives this messages and plots them in a stramlit app which reruns with each incoming message. docker-compose.yml runs the necessary services to use this solution. Eventually, we need to connect this to pub/sub, and a better way of plotting the map markers. 
