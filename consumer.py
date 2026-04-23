from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

# Consumer (lendo do topic1)
consumer = KafkaConsumer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

# Producer (enviando para topic2)
producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

try:
    topic_in = sys.argv[1]   # topic1
    topic_out = sys.argv[2]  # topic2
except:
    print('Usage: python3 consumer.py <topic_in> <topic_out>')
    exit(1)

consumer.subscribe([topic_in])

for msg in consumer:
    # 🔹 Mensagem original
    original = msg.value.decode()
    print("Received:", original)

    # 🔹 Processamento (exemplo simples)
    processed = original.upper()

    print("Processed:", processed)

    # 🔹 Envia para o novo tópico
    producer.send(topic_out, value=processed.encode())

# Garante envio
producer.flush()
