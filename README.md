# ShortTripShare
### ShortTripShare is a platform developed by BlaBlaCar 🚙
#### Our aim is to be able to share short-distance rides within cities or between towns. This platform allows users to post their short trips from point A 📍 to point B 📍, and other users who need to travel from point A to a destination B' along the way can find and join these trips. The platform facilitates the entire process, from trip posting to transaction  completion.

-   23/01/2024:
    -   jumepe [ADDED] data_gen_mvp.py:
        -   Python script containing functions to parse course data from KML file to a dataframe, generate drivers, passengers and pass a course to a driver.
-   02/02/2024:
    -   jumepe [ADDED] data_gen_driver_mvp/data_gen_passenger.py:
        -   Added data_gen_driver_mvp/data_gen_passenger.py scripts. Original script has been split in two, which can be handled separately. Driver script generates a motion with random.uniform, looping through the course points and passing the values to its location key. It passes the payload with the updated location to PubSub each loop.
        -   Passenger code generates the passenger payload with a location chosen randomly from the course.
