from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

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


def config_for_consumer():
    config = {
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET_KEY,
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanism': SSL_MACHENISM,
        'group.id': "group5",
        'auto.offset.reset': "earliest",
        'enable.auto.commit': True
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


def dict_from_restaurant(restaurant_record: dict, ctx):
    return Restaurant(restaurant_record)


def main(topic: str):
    message_read = 0
    # get schema from confluent_cloud
    schema_registry_client = SchemaRegistryClient(config_for_schema_registry())
    registered_schema_obj = schema_registry_client.get_version("restaurent-take-away-data-value", 1)
    actual_schema = registered_schema_obj.schema.schema_str

    # consumer
    kafka_consumer = Consumer(config_for_consumer())
    kafka_consumer.subscribe([topic])
    while True:
        try:
            msg = kafka_consumer.poll(1.0)
            if msg is None:
                continue
            jsondeserializer = JSONDeserializer(actual_schema, dict_from_restaurant)
            restaurant_obj = jsondeserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
            print(restaurant_obj)
            if restaurant_obj is not None:
                print(f"User record {msg.key()}: car: {restaurant_obj}")
                message_read = message_read + 1
        except KeyboardInterrupt:
            break
    print(message_read)
    kafka_consumer.close()


if __name__ == "__main__":
    topic_name = "restaurent-take-away-data"
    main(topic_name)
