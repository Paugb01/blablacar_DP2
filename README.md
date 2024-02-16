# ShortTripShare
### ShortTripShare is a platform developed by BlaBlaCar 🚙
#### Our aim is to be able to share short-distance rides within cities or between towns. This platform allows users to post their short trips from point A 📍 to point B 📍, and other users who need to travel from point A to a destination B' along the way can find and join these trips. The platform facilitates the entire process, from trip posting to transaction  completion.

-   23/01/2024:
    -   jumepe [ADDED] data_gen_mvp.py:
        -   Python script containing functions to parse course data from KML file to a dataframe, generate drivers, passengers and pass a course to a driver.

-   3/02/2024:
    -   pagaba [ADDED] mvp/visualisation:
	-   Contains a venv, producer which sends kafka messages to test streamlit live map visualisation. Consumer.py receives this messages and plots them in a stramlit app which reruns with each incoming message. docker-compose.yml runs the necessary services to use this solution. Eventually, we need to connect this to pub/sub, and a better way of plotting the map markers. 
<<<<<<< HEAD
    -
-    01/02/2024:
-       dipifa [ADDED]
-       GCP, creación de un PUB/SUB que lee los datos de un generador, utiliza DATAFLOW y se pueden hacer
-       consulstas en BIG QUERY       
=======
-   02/02/2024:
    -   jumepe [ADDED] data_gen_driver_mvp/data_gen_passenger.py:
        -   Added data_gen_driver_mvp/data_gen_passenger.py scripts. Original script has been split in two, which can be handled separately. Driver script generates a motion with random.uniform, looping through the course points and passing the values to its location key. It passes the payload with the updated location to PubSub each loop.
        -   Passenger code generates the passenger payload with a location chosen randomly from the course.
>>>>>>> main
-   11/02/2023:
    -   jumepe: [MODIFIED] df_matching.py:
        -   1.⁠ ⁠Adquiere mensajes de pubsub de driver y passenger y lo primero lo re-envia a otro topic para streamlit.
        -   2.⁠ ⁠⁠Los mensajes que recibe, los procesa y si hay match, crea un mensaje con trip_id, passenger_id, driver_id, pickup_location, dropoff_location y cost. Creé una tabla con travelled_distance, a ver si se nos ocurre como pasar puntos a km. Otra estrategia sería matchear por location Y precio, de viaje, esto se puede discutir.
        -   3.⁠ ⁠⁠Manda el mensaje de match a bigquery (osea, escribe en la tabla).
-    15/02/2023:
    -   jumepe: [MODIFIED] df_matching.py --> df_matching_blabla/taxi.py:
        -   1.⁠ Dos scripts DF para tipo blablacar (matcheo por localización Y precio), y para taxi (matcheo solo por localización).
-     16/02/2023:
    -   jumepe: [MODIFIED] data_gen_driver_mvp.py --> data_gen_driver_blabla/taxi_v1.py:
        -   1.⁠ Dos scripts de generación de driver para tipo blablacar (matcheo por localización Y precio), y para taxi (matcheo solo por localización).
