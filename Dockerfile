# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ensure storage directories exist inside container
RUN mkdir -p storage/bronze storage/silver storage/garbage storage/gold

# Set environment variable for Kafka (inside Docker network)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Default command: run the full pipeline
CMD ["python", "-m", "pipelines.run_pipeline"]
