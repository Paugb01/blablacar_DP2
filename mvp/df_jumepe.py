import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import logging
import json

class ParsePubSubMessageFn(beam.DoFn):
    def process(self, element):
        # Decodifica y mete el JSON.
        message = element.decode('utf-8')
        logging.info(f"Received message: {message}")
        try:
            msg = json.loads(message)
            # Diferencia si un mensaje viene de driver o passenger
            if 'plate_id' in msg:  # Driver 
                logging.info(f"Parsing driver message: {msg}")
                yield ('driver', msg['plate_id'], tuple(msg['location']))
            elif 'passenger_id' in msg:  # Passenger 
                logging.info(f"Parsing passenger message: {msg}")
                yield ('passenger', msg['passenger_id'], tuple(msg['location']))
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")

class MatchMessagesFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        _, messages = element
        # Compara locations de driver y passenger para encontrar coincidencias
        drivers = [m for m in messages if m[0] == 'driver']
        passengers = [m for m in messages if m[0] == 'passenger']

        for driver in drivers:
            for passenger in passengers:
                if driver[2] == passenger[2]:  # Compara ubicaciones
                    match_message = f"Coincidencia encontrada: Conductor {driver[1]} y Pasajero {passenger[1]} en {driver[2]}"
                    logging.info(match_message)
                    yield match_message

def run():
    parser = argparse.ArgumentParser(description='Streaming Dataflow Pipeline for Matching Locations.')
    parser.add_argument('--project_id', required=True, help='Nombre del proyecto de GCP.')
    parser.add_argument('--driver_subscription', required=True, help='Suscripción de PubSub para mensajes de conductores.')
    parser.add_argument('--passenger_subscription', required=True, help='Suscripción de PubSub para mensajes de pasajeros.')

    args, pipeline_args = parser.parse_known_args()

    # Configura el Pipeline, incluyendo GCP project
    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        # Lee y parsea los mensajes de drivers
        driver_msgs = (
            pipeline
            | 'Read Driver Messages' >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | 'Parse Driver Messages' >> beam.ParDo(ParsePubSubMessageFn())
        )

        # Lee y parsea los mensajes de passengers
        passenger_msgs = (
            pipeline
            | 'Read Passenger Messages' >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | 'Parse Passenger Messages' >> beam.ParDo(ParsePubSubMessageFn())
        )

        # Combina los mensajes, los agrupa por location y busca matches.
        matches = (
            (driver_msgs, passenger_msgs)
            | 'Flatten PCollections' >> beam.Flatten()
            | "Window into 10-Second Intervals" >> beam.WindowInto(beam.window.FixedWindows(10)) # Me he vuelto loco para entender BIEN por qué necesitamos establecer una ventana...
            | 'Key Messages by Location' >> beam.Map(lambda x: (x[2], x))
            | 'Group Messages by Location' >> beam.GroupByKey()
            | 'Match Messages' >> beam.ParDo(MatchMessagesFn())
        )

        # Imprime las matches encontrados.
        matches | 'Print Matches' >> beam.Map(print)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
