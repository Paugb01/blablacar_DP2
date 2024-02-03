import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import bigquery_tools



# Variables
project_id = "involuted-river-411314"
subscription_name = "dp2_driver-sub"
bq_dataset = "dp2"
bq_table = "driver"
bucket_name = "dataflow-staging-us-central1-1069963786536"

#decodificar el mensaje
def decode_message(msg):
    output = msg.decode('utf-8')
    logging.info("New PubSub Message: %s", output)
    return json.loads(output)

def transform_course_to_string(message):
    logging.info("Applying transform_course_to_string function...")
    # Verificar si la clave "course" está presente es una lista
    if 'course' in message and isinstance(message['course'], list):
        # Convertir la lista a un string
        course_str = str(message['course'])
        # Actualizar el valor de la clave "course" 
        message['course'] = course_str
    return message

def split_location(message):
    logging.info("Applying split_location function...")
    # Verificar si la clave "location" está presente en el diccionario
    if 'location' in message:
        # Obtener la tupla de coordenadas de la clave "location"
        location_tuple = message['location']
        # Asignar la primera y segunda coordenada a las variables "latitud" y "longitud"
        location_latitud, location_longitud = location_tuple
        # Actualizar el diccionario con las nuevas variables
        message['location_latitud'] = location_latitud
        message['location_longitud'] = location_longitud
        # Eliminar la clave "location" original si se desea
        del message['location']
    return message
    


def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as p:
        (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name}')
            | "decode msg" >> beam.Map(decode_message)
            | "Transform Course" >> beam.Map(transform_course_to_string)
            | "Split Location" >> beam.Map(split_location)
            | "Write to BigQuery" >>  beam.io.WriteToBigQuery(
                table=f"{project_id}:{bq_dataset}.{bq_table}",
                schema='plate_id:STRING,course:STRING,seats:INTEGER,passengers:INTEGER,trip_cost:FLOAT,full_tariff:FLOAT,location_latitud:FLOAT,location_longitud:FLOAT',
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")


        
    # Run Process
    run()