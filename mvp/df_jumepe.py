import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True

class ParsePubSubMessageFn(beam.DoFn):
    def process(self, element):
        message = element.decode('utf-8')
        try:
            msg = json.loads(message)
            logging.info("Parsed message: %s", msg)
            yield msg
        except Exception as e:
            logging.error("Failed to parse message: %s", e)

class AssignDriverFn(beam.DoFn):
    def process(self, element):
        msg = element
        location = msg.get('location')
        yield location

class AssignPassengerFn(beam.DoFn):
    def process(self, element):
        msg = element
        location = msg.get('location')
        yield location

def run():
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))
    parser.add_argument('--project_id', required=True, help='GCP cloud project name.')
    parser.add_argument('--driver_subscription', required=True, help='PubSub subscription for driver messages.')
    parser.add_argument('--passenger_subscription', required=True, help='PubSub subscription for passenger messages.')

    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id)

    with beam.Pipeline(options=options) as pipeline:
        driver_locations = (
            pipeline
            | "Read Driver Messages" >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | "Parse Driver Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Driver Locations" >> beam.ParDo(AssignDriverFn())
        )

        passenger_locations = (
            pipeline
            | "Read Passenger Messages" >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | "Parse Passenger Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Passenger Locations" >> beam.ParDo(AssignPassengerFn())
        )

        # Combine the driver and passenger locations into a single PCollection
        combined_locations = (driver_locations, passenger_locations) | beam.Flatten()

        # Define a function to compare driver and passenger locations
        def compare_locations(locations):
            driver_location, passenger_location = locations
            if driver_location == passenger_location:
                logging.info("Match: Locations match.")
            else:
                logging.info("No Match: Locations do not match.")

        # Apply the function to compare locations
        combined_locations | "Compare Locations" >> beam.Map(compare_locations)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")
    run()
