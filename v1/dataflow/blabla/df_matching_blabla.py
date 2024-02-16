import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import json
import uuid
from math import radians, cos, sin, asin, sqrt
from google.cloud import pubsub_v1

# Configuración del Publicador para Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'

def publish_location(message):
    """Publica datos de ubicación a un topic de PubSub para visualización en tiempo real"""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParseAndRepublishMessageFn(beam.DoFn):
    """Parsea mensajes de PubSub y los re-publica inmediatamente"""
    def process(self, element):
        message = element.decode('utf-8')
        logging.info(f"Mensaje recibido: {message}")
        try:
            msg = json.loads(message)
            # Re-publicar el mensaje para visualización
            publish_location(msg)
            yield msg
        except Exception as e:
            logging.error(f"Error al parsear y re-publicar mensaje: {e}")

class MatchMessagesFn(beam.DoFn):
    """Matchea conductores y pasajeros basándose en la proximidad de ubicación y compatibilidad de oferta"""
    def haversine(self, lon1, lat1, lon2, lat2):
        """Calcula la distancia Haversine entre dos puntos geográficos."""
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radio de la Tierra en km
        return c * r

    def ride_offer_within_range(self, driver_offer, passenger_offer):
        """Verifica si la oferta del pasajero es compatible con la del conductor"""
        return passenger_offer >= driver_offer * 0.75

    def process(self, element):
        _, grouped_messages = element
        for messages in grouped_messages:
            drivers = [msg for msg in messages if 'plate_id' in msg]
            passengers = [msg for msg in messages if 'passenger_id' in msg]
            for driver in drivers:
                for passenger in passengers:
                    distance = self.haversine(driver['location'][1], driver['location'][0], passenger['location'][1], passenger['location'][0])
                    if distance <= 0.00075 and self.ride_offer_within_range(driver['ride_offer'], passenger['ride_offer']):
                        match_message = {
                            'trip_id': str(uuid.uuid4()),
                            'driver_id': driver['plate_id'],
                            'passenger_id': passenger['passenger_id'],
                            'pickup_location': f"POINT({driver['location'][1]} {driver['location'][0]})",
                            'status': 'matched',
                            'cost': passenger['ride_offer']  # Suponemos que el costo es la oferta del pasajero
                        }
                        logging.info(f"MATCHEO: Conductor {driver['plate_id']} y Pasajero {passenger['passenger_id']} - Coste: €{match_message['cost']}")
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
            | 'Leer mensajes de conductores' >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | 'Parsear y re-publicar mensajes de conductores' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        passenger_msgs = (
            pipeline
            | 'Leer mensajes de pasajeros' >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | 'Parsear y re-publicar mensajes de pasajeros' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        matches = (
            (driver_msgs, passenger_msgs)
            | 'Flatten PCollections' >> beam.Flatten()
            | "Ventana (15 segundos va bien)" >> beam.WindowInto(beam.window.FixedWindows(15))
            | 'Key por Ubicación' >> beam.Map(lambda x: (x['location'], x))
            | 'Agrupar mensajes por location' >> beam.GroupByKey()
            | 'Matcheo' >> beam.ParDo(MatchMessagesFn())
        )

        matches | 'Escribir a BigQuery' >> WriteToBigQuery(
            'involuted-river-411314:dp2.trips_test',
            schema='trip_id:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, status:STRING, cost:FLOAT',
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
