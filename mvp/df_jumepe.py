import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json
import random

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
        driver_id = msg.get('plate_id')  # Extract plate_id as driver_id
        yield (driver_id, location)

class AssignPassengerFn(beam.DoFn):
    def process(self, element):
        msg = element
        location = msg.get('location')
        passenger_id = msg.get('passenger_id')  # Extract passenger_id
        yield (passenger_id, location)

def process_matches(element):
    driver_id, passenger_id = element[0], element[1]
    driver_locations = list(driver_id)
    passenger_locations = list(passenger_id)

    logging.info("Driver ID: %s", driver_id)
    logging.info("Passenger ID: %s", passenger_id)
    logging.info("Driver locations: %s", driver_locations)
    logging.info("Passenger locations: %s", passenger_locations)

    if driver_id != passenger_id:
        for driver_location in driver_locations:
            for passenger_location in passenger_locations:
                if driver_location == passenger_location:
                    logging.info("Match: Locations match.")
                    trip_id = generate_trip_id(driver_id, passenger_id)
                    logging.info("Trip ID: %s, Plate ID: %s, Passenger ID: %s", trip_id, driver_id, passenger_id)
                else:
                    logging.info("No Match: Locations do not match.")
    else:
        logging.info("No Match: Driver and passenger IDs are the same.")

def generate_trip_id(driver_id, passenger_id):
    return random.randint(1000, 9999)


def generate_trip_id(driver_id, passenger_id):
    return random.randint(1000, 9999)

def run():
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))
    parser.add_argument('--project_id', required=True, help='GCP cloud project name.')
    parser.add_argument('--driver_subscription', required=True, help='PubSub subscription for driver messages.')
    parser.add_argument('--passenger_subscription', required=True, help='PubSub subscription for passenger messages.')

    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, project=args.project_id)

    with beam.Pipeline(options=options) as pipeline:

        driver_msgs = (
            pipeline
            | "Read Driver Messages" >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | "Parse Driver Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Driver" >> beam.ParDo(AssignDriverFn())
            | "Fixed Window Driver" >> beam.WindowInto(beam.window.FixedWindows(10))  # 10 seconds window
        )

        passenger_msgs = (
            pipeline
            | "Read Passenger Messages" >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | "Parse Passenger Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Passenger" >> beam.ParDo(AssignPassengerFn())
            | "Fixed Window Passenger" >> beam.WindowInto(beam.window.FixedWindows(10))  # 10 seconds window
        )

        joined_msgs = (
            {'driver_msgs': driver_msgs, 'passenger_msgs': passenger_msgs}
            | "Merge PCollections" >> beam.CoGroupByKey()
            | "Process Matches" >> beam.Map(process_matches)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")
    run()
