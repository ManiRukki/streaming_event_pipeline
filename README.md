# ⏳ Streaming Event Pipeline

**Project Goal:** Build a real-time data pipeline that ingests events, processes them with Spark Structured Streaming, and calculates live metrics.

## 🏗 Architecture
1.  **Source:** `producer/event_producer.py` generates synthetic user click events.
2.  **Message Queue:** Apache Kafka (running via Docker).
3.  **Processing:** `consumer/spark_streaming_job.py` uses PySpark to aggregate clicks by URL in 10-second windows.
4.  **Sink:** Console (Debug) / Parquet (Production mode).

## 🚀 How to Run

### 1. Start Infrastructure (Kafka)
Ensure you have Docker installed.
```bash
docker-compose up -d
```

### 2. Install Python Deps
```bash
pip install -r requirements.txt
```

### 3. Start the Producer
Simulates web traffic.
```bash
python producer/event_producer.py
```

### 4. Start the Consumer
Processes data in real-time.
```bash
# You need Spark installed locally or submit to cluster
python consumer/spark_streaming_job.py
```

## 🛠 Tech Stack
- **Languages**: Python, PySpark
- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Infra**: Docker Compose
