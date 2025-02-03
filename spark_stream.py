import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.policies import DCAwareRoundRobinPolicy


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
                postcode, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Couldn't insert data due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception - {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # Read streaming data from Kafka topic 'users_created'
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("Initial dataframe created successfully!")

        # Print a sample of data to confirm messages are arriving in Spark
        # print("Loading data into Spark:")
        # query = spark_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        #     .writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .start()
        #
        # query.awaitTermination()  #  Run for 10 seconds to check if data appears

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(
            contact_points = ['localhost'],
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=5)

        cas_session = cluster.connect()

        return cas_session

    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to {e}")
        return None

# Function to extract structured data from Kafka's raw JSON messages
def create_selection_df_from_kafka(spark_df):
    # Define schema for incoming Kafka data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField('dob', StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
    ])
    # Parse JSON from Kafka's message value field
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka stream
        spark_df = connect_to_kafka(spark_conn)

        # Extract structured data from Kafka stream
        selection_df = create_selection_df_from_kafka(spark_df)

        # Establish connection to cassandra
        session = create_cassandra_connection()

        if session is not None:
            # Ensure keyspace and table exists in Cassandra
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # Check if data is ready to be loaded into Cassandra:
            # print("Cassandra session is started")
            # query = selection_df.writeStream \
            #     .outputMode("append") \
            #     .format("console") \
            #     .start()
            #
            # query.awaitTermination()

            print("Writing data into Cassandra")

            # Write streaming data into Cassandra
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')     # Store progress checkpoint
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()      # Keep the process running