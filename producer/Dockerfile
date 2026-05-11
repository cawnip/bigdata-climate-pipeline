FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python==2.0.2 pandas==2.1.4 pyarrow==14.0.2
COPY kafka_producer.py .
CMD ["python", "kafka_producer.py"]
