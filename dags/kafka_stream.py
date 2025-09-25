from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator   # 👈 correct import
import requests
import json


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 9, 3, 10, 0)
}


def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    return res['results'][0]


def format_data(res):
    location = res['location']
    return {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{str(location['street']['number'])} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'postcode': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }


def stream_data():
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()
    res = format_data(res)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
    #producer.flush()


with DAG(
    dag_id='kafka_stream_dag',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['kafka'],
) as dag:

    stream_task = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data,
    )
