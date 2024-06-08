from confluent_kafka import Consumer, KafkaException, KafkaError


def main():
    config_map = {
        "bootstrap.servers": "practice-kafka-1:9092",
        "client.id": "python-consumer",
        "group.id": "python-group",
        'auto.offset.reset': 'earliest',
    }

    try:
        consumer = Consumer(config_map)
    except Exception as e:
        return {"status": f"Unexpected error. {e}"}

    topics = ["teste"]

    try:
        consumer.subscribe(topics)
        print(f"Subscribed to topics: {topics}")
    except KafkaException as e:
        print(f"Failed to subscribe to topics: {e}")
        return

    try:
        while True:
            # msg = consumer.poll(timeout=1.0)
            # Consome um lote de mensagens
            messages = consumer.consume(num_messages=10, timeout=1.0)
            if not messages:
                continue
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fim da partição, não é um erro
                        print(f"Reached end of partition: {msg.error()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(f"Received message: {msg.value().decode('utf-8')}, TopicPartition: {msg.topic()} [{msg.partition()}] Offset: {msg.offset()}")

    except KeyboardInterrupt:
        print("Aborted by user")
    except KafkaException as e:
        print(f"Kafka exception: {e}")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
