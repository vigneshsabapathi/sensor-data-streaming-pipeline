FROM python:3.12-slim

WORKDIR /app

# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt setup.py ./
COPY src/ src/
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: run producer (override in docker-compose per service)
CMD ["python", "producer_main.py"]
