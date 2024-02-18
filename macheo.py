import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import json
import uuid
from math import radians, cos, sin, asin, sqrt
from google.cloud import pubsub_v1

# Configuración del Publicador para PubSub
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'  # Actualizar con el nombre de su tema

def publish_location(message):
    """Publica datos de ubicación a un topic de Pub/Sub para visualización en tiempo real."""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParseAndRepublishMessageFn(beam.DoFn):
    """Parsea mensajes de Pub/Sub y los re-publica inmediatamente."""
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

class CalculateLocationBinFn(beam.DoFn):
    """Calcula el bin de ubicación para facilitar el agrupamiento por proximidad."""
    def _init_(self, bin_size):
        self.bin_size = bin_size

    def process(self, element):
        location = element['location']
        lat, lon = location
        # Calcular índices de bin basados en el tamaño del bin
        bin_lat = int(lat / self.bin_size)
        bin_lon = int(lon / self.bin_size)
        # Usar los índices del bin como clave
        bin_key = (bin_lat, bin_lon)
        yield (bin_key, element)

class MatchMessagesFn(beam.DoFn):
    """Empareja conductores y pasajeros basándose en la compatibilidad de oferta."""
    
    def haversine(self, lon1, lat1, lon2, lat2):
        """Calcula la distancia Haversine entre dos puntos geográficos."""
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)*2 + cos(lat1) * cos(lat2) * sin(dlon/2)*2
        c = 2 * asin(sqrt(a)) 
        r = 6371  # Radio de la Tierra en km
        return c * r

    def ride_offer_within_range(self, driver_offer, passenger_offer):
        return passenger_offer >= driver_offer * 0.75

    def process(self, element):
        bin_key, messages = element
        drivers = [msg for msg in messages if 'plate_id' in msg]
        passengers = [msg for msg in messages if 'passenger_id' in msg]
        for driver in drivers:
            for passenger in passengers:
                if self.ride_offer_within_range(driver['ride_offer'], passenger['ride_offer']):
                    travelled_distance = self.haversine(driver['location'][1], driver['location'][0], passenger['dropoff_location'][1], passenger['dropoff_location'][0])
                    if passenger['ride_offer'] > 2.50:
                        match_message = {
                            'trip_id': str(uuid.uuid4()),
                            'bin_key': f"{bin_key[0]}_{bin_key[1]}",  # Incluimos el bin key para poder agrupar en BQ cuando hay múltiples match para el mismo par driver-passenger
                            'driver_id': driver['plate_id'],
                            'passenger_id': passenger['passenger_id'],
                            'pickup_location': f"POINT({passenger['location'][1]} {passenger['location'][0]})",
                            'dropoff_location': f"POINT({passenger['dropoff_location'][1]} {passenger['dropoff_location'][0]})",
                            'status': 'matched',
                            'cost': round((((passenger['ride_offer'] * 1.10) +  0.50) * 1.21), 2), # Precio total con IVA (21%) y margen industrial (Fijo + 10% ride_offer)
                            'company_margin': round(((passenger['ride_offer'] * 0.10) +  0.50), 2), # Fijo de 0.50€ más un 10% de la oferta si supera 2.50€
                            'accepted_offer': passenger['ride_offer'],
                            'travelled_distance': round(travelled_distance, 2)
                        }
                        logging.info(f"Emparejamiento encontrado: Conductor {driver['plate_id']} y pasajero {passenger['passenger_id']} - Precio: €{match_message['cost']} (gastos e IVA incluidos)")
                        yield match_message
                    else:
                        match_message = {
                            'trip_id': str(uuid.uuid4()),
                            'bin_key': f"{bin_key[0]}_{bin_key[1]}",  # Incluimos el bin key para poder agrupar en BQ cuando hay múltiples match para el mismo par driver-passenger
                            'driver_id': driver['plate_id'],
                            'passenger_id': passenger['passenger_id'],
                            'pickup_location': f"POINT({passenger['location'][1]} {passenger['location'][0]})",
                            'dropoff_location': f"POINT({passenger['dropoff_location'][1]} {passenger['dropoff_location'][0]})",
                            'status': 'matched',
                            'company_margin': 0.50, # Fijo de 0.50€
                            'cost': round(((passenger['ride_offer'] +  0.50) * 1.21), 2), # Precio total con IVA (21%) y margen industrial (Fijo)
                            'accepted_offer': passenger['ride_offer'],
                            'travelled_distance': round(travelled_distance, 2)
                        }
                        logging.info(f"Emparejamiento encontrado: Conductor {driver['plate_id']} y pasajero {passenger['passenger_id']} - Precio: €{match_message['cost']} (gastos e IVA incluidos)")
                        yield match_message

def run():
    parser = argparse.ArgumentParser(description='Pipeline de Dataflow para procesar y emparejar ubicaciones.')
    parser.add_argument('--project_id', required=True, help='ID del proyecto de GCP.')
    parser.add_argument('--driver_subscription', required=True, help='Suscripción de PubSub para mensajes de conductores.')
    parser.add_argument('--passenger_subscription', required=True, help='Suscripción de PubSub para mensajes de pasajeros.')
    parser.add_argument('--temp_location', required=True, help='Ubicación temporal para Dataflow.')
    parser.add_argument('--staging_location', required = True, help='Ubicación temporal')
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id,temp_location=args.temp_location, runner='DataflowRunner',region='europe-west6',
    staging_location=args.staging_location)
    options.view_as(StandardOptions).streaming = True

    bin_size = 0.00025  # Ajustar la tolerancia para el binning por distancia (0.00025 aprox. 30 m)

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

        messages_by_bin = (
            (driver_msgs, passenger_msgs)
            | 'Aplanar PCollections' >> beam.Flatten()
            | "Ventana de 15 s" >> beam.WindowInto(beam.window.FixedWindows(15))
            | 'Calcula valores (bins) de las keys de binneo para agrupar' >> beam.ParDo(CalculateLocationBinFn(bin_size))
            | 'Bineado' >> beam.GroupByKey()
            | 'Matcheo' >> beam.ParDo(MatchMessagesFn())
        )

        messages_by_bin | 'Escribir a BigQuery' >> WriteToBigQuery(
            'involuted-river-411314:dp2.trips',
            schema='trip_id:STRING, bin_key:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, dropoff_location:GEOGRAPHY, travelled_distance: FLOAT, status:STRING, cost:FLOAT, company_margin:FLOAT, accepted_offer:FLOAT',
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()