# Real time visualisation for DATAPROJECT 2 with streamlit.
## Usage of this folder:
The entorno folder contains the virtual environment to be used, and the necessary files to run the solution. 
For mac:
```source .venv/bin/activate``````
For windows:
idk 
### Steps:
- Create the docker compose.
```docker compose up -d```
- Install the necessary packages:
  ```pip3 install -r requirements.txt```
- In case you are just testing locally run producer and consumer:
 ```python producer.py```
 ```streamlit run consumer.py```
- In case you want to connect the visualisation to pubsub messages there might be few changes to be made. Once the message has arrived, consumer.py parses it to json (assuming the msg is a string with a valid json format). So it would only be necessary to change the location within the json where consumer is looking for parameters latitude and longitude.
