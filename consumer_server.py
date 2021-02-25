import asyncio

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "police.calls"

async def consume(broker_url, topic_name):
    consumer_properties = {"bootstrap.servers": BROKER_URL, "group.id": "0"}
    c = Consumer(consumer_properties)
    c.subscribe([TOPIC_NAME])

    while True:
        messages = c.consume(5, 1.0)
        print(f"Received {len(messages)} messages")
        for message in messages:
            if message is None:
                continue
            elif message.error() is not None:
                continue
            else:
                print(f"{message.key()}: {message.value()}")
            await asyncio.sleep(1)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(consume(BROKER_URL, TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    main()
