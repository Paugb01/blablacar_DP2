import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import json
import uuid
import math
from google.cloud import pubsub_v1

# Configuración del Publicador para Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'  # Actualiza esto con tu topic real

def publish_location(message):
    """Publica datos de ubicación a un topic de Pub/Sub para visualización en tiempo real."""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParseAndRepublishMessageFn(beam.DoFn):
    """Parsea mensajes de Pub/Sub y los re-publica inmediatamente."""
    def process(self, element):
        message = element.decode('utf-8')
        logging.info(f"Received message: {message}")
        try:
            msg = json.loads(message)
            # Re-publicar el mensaje inmediatamente para visualización
            publish_location(msg)  # Aquí se re-publica el mensaje original
            if 'plate_id' in msg:  # Mensaje de conductor
                yield ('driver', msg)
            elif 'passenger_id' in msg:  # Mensaje de pasajero
                yield ('passenger', msg)
        except Exception as e:
            logging.error(f"Failed to parse and republish message: {e}")

class MatchMessagesFn(beam.DoFn):
    """Matches drivers and passengers based on location proximity."""
    def process(self, element, window=beam.DoFn.WindowParam):
        _, messages = element
        tolerance = 0.00027027  # Approx. 30 meters in degrees

        for driver_msg in messages:
            if driver_msg[0] == 'driver':
                driver = driver_msg[1]
                driver_loc = driver['location']
                for passenger_msg in messages:
                    if passenger_msg[0] == 'passenger':
                        passenger = passenger_msg[1]
                        passenger_loc = passenger['location']
                        # Calculate the difference in coordinates
                        lat_diff = abs(driver_loc[0] - passenger_loc[0])
                        lon_diff = abs(driver_loc[1] - passenger_loc[1])
                        
                        # Check if both differences are within the tolerance
                        if lat_diff <= tolerance and lon_diff <= tolerance:
                            # Proceed with creating and yielding the match message...
                            pickup_location_wkt = f"POINT({driver_loc[1]} {driver_loc[0]})"
                            dropoff_location_wkt = f"POINT({passenger['dropoff_location'][1]} {passenger['dropoff_location'][0]})"
                            match_message = {
                                'trip_id': str(uuid.uuid4()),
                                'driver_id': driver['plate_id'],
                                'passenger_id': passenger['passenger_id'],
                                'pickup_location': pickup_location_wkt,
                                'dropoff_location': dropoff_location_wkt,
                                'status': 'matched'
                            }
                            logging.info(f"Match found within 30m: Driver {driver['plate_id']} and Passenger {passenger['passenger_id']}")
                            yield match_message

def run():
    parser = argparse.ArgumentParser(description='Pipeline de Dataflow para procesar y emparejar ubicaciones.')
    parser.add_argument('--project_id', required=True, help='ID del proyecto de GCP.')
    parser.add_argument('--driver_subscription', required=True, help='Suscripción de PubSub para mensajes de conductores.')
    parser.add_argument('--passenger_subscription', required=True, help='Suscripción de PubSub para mensajes de pasajeros.')
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        driver_msgs = (
            pipeline
            | 'Read Driver Messages' >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | 'Parse and Republish Driver Messages' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        passenger_msgs = (
            pipeline
            | 'Read Passenger Messages' >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | 'Parse and Republish Passenger Messages' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        matches = (
            (driver_msgs, passenger_msgs)
            | 'Flatten PCollections' >> beam.Flatten()
            | "Window into 10-Second Intervals" >> beam.WindowInto(beam.window.FixedWindows(10))
            | 'Key Messages by Location' >> beam.Map(lambda x: (x[1]['location'], x))
            | 'Group Messages by Location' >> beam.GroupByKey()
            | 'Match Messages' >> beam.ParDo(MatchMessagesFn())
        )

        # Almacenar registros coincidentes en BigQuery
        matches | 'Write to BigQuery' >> WriteToBigQuery(
            'involuted-river-411314:dp2.trips_test',
            schema='trip_id:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, status:STRING',
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
