import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airscholar',    # Owner of the DAG
    'start_date': datetime(2025, 1, 27, 11, 30)
}

# Function to fetch random user data from an external API
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()            # Convert response to json
    res = res['results'][0]     # Extract the first user from results

    return res

# Function to format fetched user data into a structured dictionary
def format_data(res):
    data = {}
    location = res['location']

    data['id'] = str(uuid.uuid4())    # TypeError: Object of type UUID is not JSON serializable

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']   # dob - Date of Birth
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# Function to continuously stream user data to a Kafka Topic
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    # Create Topic in Kafka
    # res = get_data()
    # res = format_data(res)
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    # producer.send('users_created', json.dumps(res).encode('utf-8'))
    # print(json.dumps(res, indent=3))

    # Kafka producer setup with specified broker
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Stop streaming after 1 minute
            break
        try:
            # Fetch and format data
            res = get_data()
            res = format_data(res)

            # Send formatted data to Kafka topic
            producer.send('users_created', json.dumps(res, default=str).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue    # Continue even if errors occurs

# Define and Airflow DAG
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:     # Prevent DAG from running past missed executions

    # Create a PythonOperator task that runs the stream_data function
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',     # Unique task ID
        python_callable=stream_data
    )

# stream_data()