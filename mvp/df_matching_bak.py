import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import json
import uuid
from google.cloud import pubsub_v1

# Configuración del Publicador para Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'  # Actualiza esto con tu topic real

def publish_location(message):
    """Publica datos de ubicación a un topic de Pub/Sub para visualización en tiempo real."""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParsePubSubMessageFn(beam.DoFn):
    """Parsea mensajes de Pub/Sub, identificando si son de conductores o pasajeros."""
    def process(self, element):
        message = element.decode('utf-8')
        logging.info(f"Received message: {message}")
        try:
            msg = json.loads(message)
            if 'plate_id' in msg:  # Mensaje de conductor
                yield ('driver', msg['plate_id'], tuple(msg['location']))
            elif 'passenger_id' in msg:  # Mensaje de pasajero
                yield ('passenger', msg['passenger_id'], tuple(msg['location']))
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")

class MatchMessagesFn(beam.DoFn):
    """Busca coincidencias entre conductores y pasajeros basándose en su ubicación."""
    def process(self, element, window=beam.DoFn.WindowParam):
        _, messages = element
        drivers = [msg for msg in messages if msg[0] == 'driver']
        passengers = [msg for msg in messages if msg[0] == 'passenger']

        for driver in drivers:
            for passenger in passengers:
                if driver[2] == passenger[2]:  # Compara ubicaciones
                    # Convertir ubicación a WKT
                    pickup_location_wkt = f"POINT({driver[2][1]} {driver[2][0]})"
                    match_message = {
                        'trip_id': str(uuid.uuid4()),
                        'driver_id': driver[1],
                        'passenger_id': passenger[1],
                        'pickup_location': pickup_location_wkt,
                        'status': 'matched'
                    }
                    # Publicar coincidencia para visualización y almacenamiento
                    logging.info(f"Match found: Driver {driver[1]} and Passenger {passenger[1]} at {pickup_location_wkt}")
                    publish_location(match_message)
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
            | 'Parse Driver Messages' >> beam.ParDo(ParsePubSubMessageFn())
        )

        passenger_msgs = (
            pipeline
            | 'Read Passenger Messages' >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | 'Parse Passenger Messages' >> beam.ParDo(ParsePubSubMessageFn())
        )

        matches = (
            (driver_msgs, passenger_msgs)
            | 'Flatten PCollections' >> beam.Flatten()
            | "Window into 10-Second Intervals" >> beam.WindowInto(beam.window.FixedWindows(10))
            | 'Key Messages by Location' >> beam.Map(lambda x: (x[2], x))
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
