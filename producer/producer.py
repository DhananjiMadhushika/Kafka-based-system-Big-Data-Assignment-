from confluent_kafka import Producer
from fastavro import parse_schema, writer
import io, json, random, time, logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema = json.load(open("order.avsc", "r"))
parsed_schema = parse_schema(schema)

def get_kafka_producer(max_retries=10, retry_delay=5):
    for attempt in range(max_retries):
        try:
            p = Producer({
                "bootstrap.servers": "kafka:9092",
                "socket.timeout.ms": 10000,
                "message.timeout.ms": 5000
            })
            
            # Test connection
            p.list_topics(timeout=10)
            logger.info("Successfully connected to Kafka")
            return p
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

p = get_kafka_producer()

def serialize_avro(data):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, parsed_schema, [data])
    return bytes_writer.getvalue()

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    order = {
        "orderId": str(random.randint(1000, 9999)),
        "product": f"Item{random.randint(1,5)}",
        "price": float(random.randint(100, 500))
    }

    try:
        p.produce("orders", serialize_avro(order), callback=delivery_report)
        p.poll(0)  # Serve delivery callbacks
        logger.info(f"Produced: {order}")
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")

    time.sleep(1)