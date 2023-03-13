from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from uuid import uuid4
import pandas as pd

# configurations for accessing topics from kafka brokers
API_KEY = '5C26WEZGRMGOTEOB'
API_SECRET_KEY = 'GOJhYf9NLABpYh/pcq6Ymla0HtfAoq9H0rJ4c9mT1CSWf1IT5vBSfdhfPMYyfz2p'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'

# configurations for accessing schema_registry
ENDPOINT_SCHEMA_URL = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'LXD7HMNT5OSTFQ23'
SCHEMA_REGISTRY_API_SECRET = 'sz6Rp5JlPnLvPPa5H99nlXn6/e7FNc8mkAlkptCabjViKBsD4WYqvhinTlR4lG6S'

# Security configurations
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

columns = ["Order Date", "Item Name", "Quantity", "Product Price", "Total products"]


def config_for_producer():
    config = {
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET_KEY,
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanism': SSL_MACHENISM
    }
    return config


def config_for_schema_registry():
    config = {
        'url': ENDPOINT_SCHEMA_URL,
        'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    }
    return config


class Restaurant:
    def __init__(self, record: dict):
        self.record = record


def get_restaurant_instance(file):
    df = pd.read_csv(file)
    df = df.iloc[:, 1:]
    restaurants = []
    for data in df.values:
        print(data)
        restaurant = Restaurant(dict(zip(columns, data)))
        restaurants.append(restaurant)
        yield restaurant


def restaurant_to_dict(restaurant, ctx):
    return restaurant.record


def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for user record --> {msg.keys()},{err}')
        return
    print(
        f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def main(topic: str):
    # get schema from confluent_cloud
    schema_registry_client = SchemaRegistryClient(config_for_schema_registry())
    registered_schema_obj = schema_registry_client.get_version("restaurent-take-away-data-value", 1)
    actual_schema = registered_schema_obj.schema.schema_str

    # serialize the keys
    string_serializer = StringSerializer("utf_8")

    # serialize the values
    json_serializer = JSONSerializer(actual_schema, schema_registry_client, restaurant_to_dict)

    # producer
    kafka_producer = Producer(config_for_producer())
    try:
        file_path = 'H:\Data engineering Assignments\Kafka Assignments/restaurant_orders.csv'
        for restaurant in get_restaurant_instance(file_path):
            print(restaurant)
            kafka_producer.produce(
                topic,
                key=string_serializer(str(uuid4())),
                value=json_serializer(restaurant,
                                      SerializationContext(topic, MessageField.VALUE)),

                on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    kafka_producer.poll(5.0)

    print("\nFlushing records...")

    kafka_producer.flush()


if __name__ == "__main__":
    topic_name = "restaurent-take-away-data"
    main(topic_name)
