# DATAPROJECT 2 - EDEM - Masters in Big Data & Cloud
# 

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
            publish_location(msg)  # Mensaje original
            if 'plate_id' in msg:  # Mensaje de conductor
                yield ('driver', msg)
            elif 'passenger_id' in msg:  # Mensaje de pasajero
                yield ('passenger', msg)
        except Exception as e:
            logging.error(f"Failed to parse and republish message: {e}")

class MatchMessagesFn(beam.DoFn):
    """Matches drivers and passengers based on location proximity and calculates the trip details."""

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance in kilometers between two points 
        on the earth (specified in decimal degrees).
        """
        # Convert decimal degrees to radians 
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # Haversine formula 
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371  # Radius of earth in kilometers
        return c * r

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
                        lat_diff = abs(driver_loc[0] - passenger_loc[0])
                        lon_diff = abs(driver_loc[1] - passenger_loc[1])
                        
                        if lat_diff <= tolerance and lon_diff <= tolerance:
                            dropoff_loc = passenger['dropoff_location']
                            travelled_distance = self.haversine(driver_loc[1], driver_loc[0], dropoff_loc[1], dropoff_loc[0])
                            cost = travelled_distance * 0.08  # Cost calculation: €0.08/km
                            
                            match_message = {
                                'trip_id': str(uuid.uuid4()),
                                'driver_id': driver['plate_id'],
                                'passenger_id': passenger['passenger_id'],
                                'pickup_location': f"POINT({driver_loc[1]} {driver_loc[0]})",
                                'dropoff_location': f"POINT({dropoff_loc[1]} {dropoff_loc[0]})",
                                'status': 'dropped off',  # Updated status
                                'travelled_distance': travelled_distance,
                                'cost': cost
                            }
                            logging.info(f"Match and drop-off processed: Driver {driver['plate_id']} and Passenger {passenger['passenger_id']} - Distance: {travelled_distance} km, Cost: €{cost}")
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
            schema='trip_id:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, dropoff_location:GEOGRAPHY, status:STRING, travelled_distance:FLOAT, cost:FLOAT',
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
