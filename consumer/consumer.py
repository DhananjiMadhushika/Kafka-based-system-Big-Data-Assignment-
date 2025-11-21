from confluent_kafka import Consumer, Producer
from fastavro import reader
import io, time, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_kafka_consumer(max_retries=10, retry_delay=5):
    for attempt in range(max_retries):
        try:
            consumer = Consumer({
                "bootstrap.servers": "kafka:9092",
                "group.id": "order-group",
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 10000,
                "socket.timeout.ms": 10000
            })
            
            consumer.list_topics(timeout=10)
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

def get_kafka_producer(max_retries=10, retry_delay=5):
    for attempt in range(max_retries):
        try:
            producer = Producer({
                "bootstrap.servers": "kafka:9092",
                "socket.timeout.ms": 10000,
                "message.timeout.ms": 5000
            })
            logger.info("Successfully connected to Kafka producer")
            return producer
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

consumer = get_kafka_consumer()
dlq = get_kafka_producer()

consumer.subscribe(["orders"])

total_price = 0
count = 0

def deserialize_avro(bytes_data):
    bytes_reader = io.BytesIO(bytes_data)
    avro_reader = reader(bytes_reader)
    for record in avro_reader:
        return record

def process_message(msg):
    global total_price, count

    order = deserialize_avro(msg)
    logger.info(f"Consumed: {order}")

    total_price += order["price"]
    count += 1

    logger.info(f"Running Average = {total_price / count}")

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        retries = 3

        while retries > 0:
            try:
                process_message(msg.value())
                break
            except Exception as e:
                logger.warning(f"Retrying... {retries}, error: {e}")
                retries -= 1
                time.sleep(1)

        if retries == 0:
            logger.warning("Sending to DLQ")
            try:
                dlq.produce("dlq-orders", msg.value())
                dlq.flush()
            except Exception as e:
                logger.error(f"Failed to send to DLQ: {e}")

    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
        time.sleep(5)

consumer.close()