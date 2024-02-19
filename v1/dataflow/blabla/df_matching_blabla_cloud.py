import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import sys
import json
import uuid
from math import radians, cos, sin, asin, sqrt
from google.cloud import pubsub_v1

# Initialize the Publisher for PubSub
publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/involuted-river-411314/topics/dp2_streamlit_test'  # Update with your actual topic name

def publish_location(message):
    """Publishes location data to a Pub/Sub topic for real-time visualization."""
    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_name, message_bytes)

class ParseAndRepublishMessageFn(beam.DoFn):
    """Parses Pub/Sub messages and republishes them immediately."""
    def process(self, element):
        message = element.decode('utf-8')
        logging.info(f"Received message: {message}")
        try:
            msg = json.loads(message)
            # Republish the message for visualization
            publish_location(msg)
            yield msg
        except Exception as e:
            logging.error(f"Failed to parse and republish message: {e}")

class CalculateLocationBinFn(beam.DoFn):
    """Calculates location bin to facilitate grouping by proximity."""
    def __init__(self, bin_size):
        self.bin_size = bin_size

    def process(self, element):
        location = element['location']
        lat, lon = location
        # Calculate bin indices based on bin size
        bin_lat = int(lat / self.bin_size)
        bin_lon = int(lon / self.bin_size)
        # Use bin indices as key
        bin_key = (bin_lat, bin_lon)
        yield (bin_key, element)

class MatchMessagesFn(beam.DoFn):
    """Matches drivers and passengers based on offer compatibility."""
    def haversine(self, lon1, lat1, lon2, lat2):
        """Calculates Haversine distance between two geographic points."""
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371  # Radius of Earth in kilometers
        return c * r

    def process(self, element):
        bin_key, messages = element
        drivers = [msg for msg in messages if 'plate_id' in msg]
        passengers = [msg for msg in messages if 'passenger_id' in msg]
        for driver in drivers:
            for passenger in passengers:
                driver_location = driver['location']
                passenger_dropoff_location = passenger['dropoff_location']
                travelled_distance = self.haversine(driver_location[1], driver_location[0], passenger_dropoff_location[1], passenger_dropoff_location[0])
                match_message = {
                    'trip_id': str(uuid.uuid4()),
                    'bin_key': f"{bin_key[0]}_{bin_key[1]}",
                    'driver_id': driver['plate_id'],
                    'passenger_id': passenger['passenger_id'],
                    'pickup_location': f"POINT({driver_location[1]} {driver_location[0]})",
                    'dropoff_location': f"POINT({passenger_dropoff_location[1]} {passenger_dropoff_location[0]})",
                    'travelled_distance': round(travelled_distance, 2),
                    # Add any other fields as necessary
                }
                yield match_message

def run(argv=None):
    parser = argparse.ArgumentParser(description='Dataflow Pipeline for Matching Locations')
    parser.add_argument('--project_id', required=True, help='GCP project ID.')
    parser.add_argument('--driver_subscription', required=True, help='PubSub subscription for driver messages.')
    parser.add_argument('--passenger_subscription', required=True, help='PubSub subscription for passenger messages.')
    parser.add_argument('--temp_location', required=True, help='GCS path for temporary files.')
    parser.add_argument('--template_location', required=True, help='GCS path to store the template.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project=known_args.project_id,
        temp_location=known_args.temp_location,
        template_location=known_args.template_location,
        streaming=True
    )

    with beam.Pipeline(options=options) as pipeline:
        driver_msgs = (
            pipeline
            | 'Read Driver Messages' >> beam.io.ReadFromPubSub(subscription=known_args.driver_subscription)
            | 'Parse and Republish Driver Messages' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        passenger_msgs = (
            pipeline
            | 'Read Passenger Messages' >> beam.io.ReadFromPubSub(subscription=known_args.passenger_subscription)
            | 'Parse and Republish Passenger Messages' >> beam.ParDo(ParseAndRepublishMessageFn())
        )

        messages_by_bin = (
            (driver_msgs, passenger_msgs)
            | 'Flatten PCollections' >> beam.Flatten()
            | 'Window into Fixed Intervals' >> beam.WindowInto(beam.window.FixedWindows(15))  # Adjust window size as needed
            | 'Calculate Bin Values' >> beam.ParDo(CalculateLocationBinFn(bin_size=0.00025))  # Adjust bin size as needed
            | 'Group by Bin' >> beam.GroupByKey()
            | 'Match Messages' >> beam.ParDo(MatchMessagesFn())
        )

        # Define your BigQuery table schema
        table_schema = 'trip_id:STRING, bin_key:STRING, driver_id:STRING, passenger_id:STRING, pickup_location:GEOGRAPHY, dropoff_location:GEOGRAPHY, travelled_distance:FLOAT'

        messages_by_bin | 'Write to BigQuery' >> WriteToBigQuery(
            table='involuted-river-411314:dp2.trips',
            schema=table_schema,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
