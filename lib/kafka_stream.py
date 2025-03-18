from confluent_kafka import Producer
import time

producer = Producer(
    {
        "bootstrap.servers": "localhost:9092"
    }
)


def send_to_kafka(partition):
    """Send records to Kafka with a 10-second interval."""
    for row in partition:
        key = row["key"]
        value = row["value"]
        producer.produce("sbdl_topic", key=key, value=value)
        print(f"Sent payload : {value}")

        producer.flush()
        time.sleep(2)


def write_kafka(df):
    df.rdd.foreachPartition(send_to_kafka)
