# Data_Pipeline-Realtime_Data_Streaming

This project demonstrates a data engineering pipeline where data is extracted from an external API, processed in real-time using Apache Kafka and Apache Spark, and ultimately stored in a Cassandra database. The entire pipeline is containerized using Docker for easy deployment and scalability.

## System Architecture
![System Architecture](System_Architecture.png)

### Key Components:
- **Data Source**: We use https://randomuser.me API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center & Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.
- **Docker**: Containerizes all components for seamless integration and deployment.

---

## Workflow

1. **Data Extraction**:
   - Apache Airflow triggers periodic extraction of user data from the Random User API.
   - Extracted data is streamed to Apache Kafka.

2. **Data Streaming**:
   - Kafka acts as the central streaming platform, with topics configured to hold the data.
   - Kafka’s Schema Registry ensures consistent data serialization and deserialization.

3. **Data Processing**:
   - Apache Spark processes the streamed data in real-time using Spark Streaming.
   - Spark transforms the raw data into a structured format suitable for storage.

4. **Data Storage**:
   - Processed data is streamed to a Cassandra database for storage and querying.

---

## Prerequisites

- **Docker Desktop**: Ensure Docker Desktop is installed and running on your system.
- **Python**: Run the `requirements.txt` file from the repository to install the necessary dependencies:
  ```bash
  pip install -r requirements.txt
  ```
  
---

## Setup and Deployment

1. **Clone the Repository**:
   ```bash
   git clone git@github.com:DZA-Mirai/Data_Pipeline-Realtime_Data_Streaming.git
   ```

2. **Update Configuration Files**:
   - Configure Kafka topic.

3. **Start Docker Containers**:
   ```bash
   docker-compose up -d
   ```

4. **Run the Pipeline**:
   - Trigger the Airflow DAG to start data extraction and processing.

5. **Monitor the Pipeline**:
   - Use the Kafka Control Center to monitor message flow.
   - Check Spark logs for processing details.
   - Verify data storage in Cassandra using cqlsh or a compatible client.

---

## Technologies Used

- **Apache Kafka**: Real-time message broker.
- **Apache Spark**: Distributed data processing framework.
- **Apache Cassandra**: NoSQL database for scalable storage.
- **Apache Airflow**: Workflow orchestration.
- **PostgreSQL**: Metadata storage for Airflow.
- **Apache ZooKeeper**: Kafka broker coordination and management.
- **Docker**: Containerization platform.