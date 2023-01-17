from confluent_kafka import Consumer, KafkaError
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

# Create the consumer
consumer = Consumer(conf)

try:
    # Subscribe to the topic
    consumer.subscribe(['demo'])

    # Continuously poll for new messages
    while True:
        msg = consumer.poll(1.0)

        if msg is None: 
            print('No message received')
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print("Reached end of topic {}, partition {}".format(msg.topic(), msg.partition()))
            else:
                # Error
                print("Error: {}".format(msg.error()))
        else:
            # Message received. Only message
            print("Received message: {}".format(msg.value().decode('utf-8')))

            # Acknowledge the message
            consumer.commit(message=msg)

except Exception as e:
    print("Error: {}".format(e))
    consumer.close()


