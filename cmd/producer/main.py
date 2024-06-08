from confluent_kafka import Producer, KafkaError

def NewKafkaProducer():
    config_map = {
        "bootstrap.servers": "practice-kafka-1:9092",
        "acks": "all"
    }

    try:
        producer = Producer(config_map)
        return producer
    except Exception as e:
        return {"status": f"Unexpected error. {e}"}


def publish(msg, topic, producer, key, delivery_report):
    def delivery_callback(err, msg):
        if err is not None:
            delivery_report(err, msg)
        else:
            delivery_report(None, msg)

    try:
        producer.produce(
            topic=topic,
            key=key,
            value=msg,
            callback=delivery_callback
        )
        producer.poll(0)  # Trigger delivery callback

        producer.flush()  # Ensure all messages are delivered
    except KafkaError as e:
        return e
    return None


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic: {msg.topic()}, partition: [{msg.partition()}], offset: {msg.offset()}")


def main():
    producer = NewKafkaProducer()

    error = publish("transferiu", "teste", producer, "transferencia2", delivery_report)
    if error:
        print(f"Publish error: {error}")


if __name__ == "__main__":
    main()
