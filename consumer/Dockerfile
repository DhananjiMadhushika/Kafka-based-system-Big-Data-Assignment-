FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install confluent-kafka fastavro

CMD ["python", "consumer.py"]
