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

class AssignDriverLocationFn(beam.DoFn):
    def process(self, element):
        msg = element
        location = msg.get('location')
        yield location

class AssignPassengerLocationFn(beam.DoFn):
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
        driver_location = None
        passenger_location = None

        driver_msgs = (
            pipeline
            | "Read Driver Messages" >> beam.io.ReadFromPubSub(subscription=args.driver_subscription)
            | "Parse Driver Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Driver Location" >> beam.ParDo(AssignDriverLocationFn())
            | "Log Driver Location" >> beam.Map(lambda location: globals().update({'driver_location': location}) or logging.info("Driver location: %s", location))
        )

        passenger_msgs = (
            pipeline
            | "Read Passenger Messages" >> beam.io.ReadFromPubSub(subscription=args.passenger_subscription)
            | "Parse Passenger Messages" >> beam.ParDo(ParsePubSubMessageFn())
            | "Assign Passenger Location" >> beam.ParDo(AssignPassengerLocationFn())
            | "Log Passenger Location" >> beam.Map(lambda location: globals().update({'passenger_location': location}) or logging.info("Passenger location: %s", location))
            | "Compare Locations" >> beam.Map(lambda _: logging.info("Match: Locations match.") if driver_location == passenger_location else logging.info("No Match: Locations do not match."))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")
    run()
