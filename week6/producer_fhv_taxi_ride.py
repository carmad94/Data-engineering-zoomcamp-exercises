from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
import pandas as pd


def load_avro_schema_from_file():
    key_schema = avro.load("fhv_taxi_ride_key.avsc")
    value_schema = avro.load("fhv_taxi_ride_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(
        producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema
    )

    fhv_taxi = pd.read_csv("data/fhv_tripdata_2019-01.csv.gz")
    fhv_taxi = fhv_taxi[(fhv_taxi["PUlocationID"].notna()) & (fhv_taxi["DOlocationID"].notna())]
    for index, row in fhv_taxi.iterrows():
        key = {
            "fhvId": f"{row['dispatching_base_num']}-{row['pickup_datetime']}"
        }
        
        value = {
            "dispatching_base_num": row['dispatching_base_num'],
            "pickup_datetime": row['pickup_datetime'],
            "dropoff_datetime": row['dropOff_datetime'],
            "PULocationID": int(row['PUlocationID']),
            "DOLocationID": int(row['DOlocationID']),
            "affiliated_base_number": row['Affiliated_base_number']
        }

        try:
            producer.produce(
                topic='rides_fhv',
                key=key,
                value=value
            )
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()