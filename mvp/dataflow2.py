import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from datetime import timedelta

# Variables
project_id = "involuted-river-411314"
subscription_name = "dp2_driver-sub"
subscription_name2 = "dp2_passenger-sub"
bq_dataset = "dp2"
bq_table = "driver"
bucket_name = "dataflow-staging-us-central1-1069963786536"

# Decode the message
def decode_message(msg):
    output = msg.decode('utf-8')
    return json.loads(output)

# Extract location from the message
def extract_location(message):
    if 'location' in message:
        return message['location']
    return None

def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as p:
        driver_locations = (
            p 
            | "ReadFromPubSub1" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name}')
            | "Decode msg" >> beam.Map(decode_message)
            | "Extract Location for Driver" >> beam.Map(lambda x: ('driver', extract_location(x)) if 'plate_id' in x else None)  # Extract location only for messages with 'plate_id'
            | "Filter None Values" >> beam.Filter(lambda x: x is not None)  # Remove None values resulting from messages without 'plate_id'
            | "Assign Driver Windows" >> beam.WindowInto(FixedWindows(size=int(timedelta(minutes=1).total_seconds()))))  # Adjust window size as per your requirement

        passenger_locations = (
            p
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name2}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Extract Location for Passenger" >> beam.Map(lambda x: ('passenger', extract_location(x)))
            | "Assign Passenger Windows" >> beam.WindowInto(FixedWindows(size=int(timedelta(minutes=1).total_seconds()))))  # Adjust window size as per your requirement

        # CoGroupByKey to ensure the same key for driver and passenger locations
        merged_locations = (
            {'driver': driver_locations, 'passenger': passenger_locations}
            | "Merge Locations" >> beam.CoGroupByKey()
        )

        def log_and_return(item):
            logging.info("Merged Locations: %s", item)
            return item

        def filter_matching_locations(item):
            logging.info("Checking if driver and passenger locations match:")
            logging.info("Driver locations: %s", item[1]['driver'])
            logging.info("Passenger locations: %s", item[1]['passenger'])
            
            driver_location = item[1]['driver']
            passenger_location = item[1]['passenger']
            
            # Check if both driver and passenger locations are non-empty lists
            if driver_location and passenger_location and len(driver_location) > 0 and len(passenger_location) > 0:
                match = driver_location[0] == passenger_location[0]
                logging.info("Coordinates comparison result: %s", match)
            else:
                match = False
            
            logging.info("Locations match: %s", match)
            return match

        matched_locations = (
            merged_locations
            | "Log Merged Locations" >> beam.Map(log_and_return)
            | "Filter Matching Locations" >> beam.Filter(filter_matching_locations)
            | "Get Matching Locations" >> beam.Map(lambda x: x[1]['driver'])
        )

        matched_locations | "Print Matching Locations" >> beam.Map(print)


if __name__ == '__main__':
    # Set Logs
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")

    # Run Process
    run()
