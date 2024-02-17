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
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'  # Actualizar con topic final...

def publish_location(message):
    """Publica datos de ubicación a un topic de PubSub para Streamlit"""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParseAndRepublishMessageFn(beam.DoFn):
    """Parsea mensajes de PubSub y los re-publica inmediatamente"""
    def process(self, element):
        message = element.decode('utf-8')
        logging.info(f"Received message: {message}")
        try:
            msg = json.loads(message)
            # Re-publicar el mensaje inmediatamente para visualización
            publish_location(msg)  # Mensaje original
            if 'plate_id' in msg or 'passenger_id' in msg:  # Mensaje de conductor o pasajero
                yield msg
        except Exception as e:
            logging.error(f"Failed to parse and republish message: {e}")

class MatchMessagesFn(beam.DoFn):
    """Matchea drivers y passengers con una tolerancia a la posición y compatibilidad de oferta de viaje."""

    def haversine(self, lon1, lat1, lon2, lat2):
        """Calcula la distancia entre dos puntos en la Tierra usando la fórmula Haversine."""
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)*2 + cos(lat1) * cos(lat2) * sin(dlon/2)*2
        c = 2 * asin(sqrt(a)) 
        r = 6371  # Radio de la Tierra en kilómetros.
        return c * r

    def ride_offer_within_range(self, driver_offer, passenger_offer):
        """Verifica si las ofertas de viaje están dentro del ±25% de diferencia."""
        lower_bound = passenger_offer * 0.75
        upper_bound = passenger_offer * 1.25
        return lower_bound <= driver_offer <= upper_bound

    def process(self, element, window=beam.DoFn.WindowParam):
        _, messages = element
        tolerance = 0.00027027  # Aprox. 30 metros en grados.

        for driver_msg in messages:
            if driver_msg.get('plate_id'):  # Mensaje de driver
                driver_loc = driver_msg['location']
                driver_offer = driver_msg['ride_offer']
                for passenger_msg in messages:
                    if passenger_msg.get('passenger_id'):  # Mensaje de passenger
                        passenger_loc = passenger_msg['location']
                        passenger_offer = passenger_msg['ride_offer']
                        
                        if self.ride_offer_within_range(driver_offer, passenger_offer):
                            lat_diff = abs(driver_loc[0] - passenger_loc[0])
                            lon_diff = abs(driver_loc[1] - passenger_loc[1])
                            
                            if lat_diff <= tolerance and lon_diff <= tolerance:
                                dropoff_loc = passenger_msg['dropoff_location']
                                cost = min(driver_offer, passenger_offer)  # Lógica para decidir qué oferta usar
                                
                                match_message = {
                                    'trip_id': str(uuid.uuid4()),
                                    'driver_id': driver_msg['plate_id'],
                                    'passenger_id': passenger_msg['passenger_id'],
                                    'pickup_location': f"POINT({driver_loc[1]} {driver_loc[0]})",
                                    'dropoff_location': f"POINT({dropoff_loc[1]} {dropoff_loc[0]})",
                                    'status': 'dropped off',
                                    'straight_distance': self.haversine(driver_loc[1], driver_loc[0], dropoff_loc[1], dropoff_loc[0]),
                                    'cost': cost
                                }
                                logging.info(f"Match y drop-off procesado: Driver {driver_msg['plate_id']} y passenger {passenger_msg['passenger_id']} - Straight Distance: {match_message['straight_distance']} km, Cost: €{cost}")
                                yield match_message

def run():
    parser = argparse.ArgumentParser(description='Pipeline de Dataflow para procesar y emparejar ubicaciones.')
    parser.add_argument('--project_id', required=True, help='ID del proyecto de GCP.')
    parser.add_argument('--driver_subscription', required=True, help='Suscripción de PubSub para mensajes de conductores.')
    parser.add_argument('--passenger_subscription', required=True, help='Suscripción de PubSub para mensajes de pasajeros.')
    parser.add_argument('--temp_location', required=True, help='Ubicación temporal para Dataflow.')
    parser.add_argument('--staging_location', required = True, help='Ubicación temporal')
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id, temp_location=args.temp_location, runner='DataflowRunner',region='europe-west6',
    staging_location=args.staging_location)
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
            | 'Key Messages by Shared Attribute' >> beam.Map(lambda x: ((x['location'], x['ride_offer']), x))
            | 'Group Messages by Shared Attributes' >> beam.GroupByKey()
            | 'Match Messages' >> beam.ParDo(MatchMessagesFn())
        )

        # Almacenar registros coincidentes en BigQuery
        matches | 'Write to BigQuery' >> WriteToBigQuery(
            'involuted-river-411314:dp2.trips_test',
            schema='trip_id:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, dropoff_location:GEOGRAPHY, status:STRING, straight_distance:FLOAT, cost:FLOAT',
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

