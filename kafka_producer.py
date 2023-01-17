from confluent_kafka import Producer, KafkaError
import os
from dotenv import load_dotenv

# load the environment variables from .env file
load_dotenv()

# Define the configuration
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP.SERVERS'),
    'security.protocol': os.getenv('SECURITY.PROTOCOL'),
    'sasl.mechanisms': os.getenv('SASL.MECHANISMS'),
    'ssl.ca.location': None,
    'sasl.username': os.getenv('SASL.USERNAME'),
    'sasl.password': os.getenv('SASL.PASSWORD'),
    'session.timeout.ms': int(os.getenv('SESSION.TIMEOUT.MS')),
    'group.id': os.getenv('GROUP.ID'),
    'enable.auto.commit': bool(os.getenv('ENABLE.AUTO.COMMIT'))
}

print(conf)

# Create the producer
producer = Producer(conf)

# Function to send message to the topic
def send_message(topic, key, value):
    try:
        # Send the message
        producer.produce(topic, key=key, value=value)
        producer.flush()
        print("Sent message: topic:{}, key:{}, value:{}".format(topic, key, value))
    except KafkaError as e:
        print("Error: {}".format(e))


try:
    # Continuously poll for new messages
    while True:

        message_raw = input() 
        message = (message_raw.encode('utf-8'))

        # Send a message
        try:
            send_message("demo", "mulesoft", message)
        except KafkaError as e:
            print("Error: {}".format(e))

except Exception as e:
    print("Error: {}".format(e))





                                




