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
    """Busca coincidencias entre conductores y pasajeros basándose en su ubicación."""
    def process(self, element, window=beam.DoFn.WindowParam):
        _, messages = element
        drivers = [msg for msg in messages if msg[0] == 'driver']
        passengers = [msg for msg in messages if msg[0] == 'passenger']

        for driver in drivers:
            for passenger in passengers:
                # Assuming the location comparison for a match is still valid
                if driver[2] == passenger[2]:  # Compara ubicaciones
                    # Convertir ubicación a WKT para BigQuery
                    pickup_location_wkt = f"POINT({driver[2][1]} {driver[2][0]})"
                    # Assuming passenger message contains 'dropoff_location' field
                    dropoff_location = passenger[1].get('dropoff_location', None)
                    # Convert dropoff_location to WKT if it's not None and is a list or tuple
                    if dropoff_location and isinstance(dropoff_location, (list, tuple)):
                        dropoff_location_wkt = f"POINT({dropoff_location[1]} {dropoff_location[0]})"
                    else:
                        dropoff_location_wkt = None

                    match_message = {
                        'trip_id': str(uuid.uuid4()),
                        'driver_id': driver[1],
                        'passenger_id': passenger[1],
                        'pickup_location': pickup_location_wkt,
                        'dropoff_location': dropoff_location_wkt,
                        'status': 'matched'
                    }
                    # Publicar coincidencia para visualización y almacenamiento
                    logging.info(f"Match found: Driver {driver[1]} and Passenger {passenger[1]} at {pickup_location_wkt}")
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
