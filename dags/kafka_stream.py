import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    """
    Formate les données récupérées depuis l'API Random User.
    """
    data = {
        "id": str(uuid.uuid4()),  # Convertir UUID en chaîne de caractères
        "first_name": res["name"]["first"],
        "last_name": res["name"]["last"],
        "gender": res["gender"],
        "address": f"{res['location']['street']['number']} {res['location']['street']['name']}",
        "post_code": res["location"]["postcode"],
        "email": res["email"],
        "username": res["login"]["username"],
        "dob": res["dob"]["date"],
        "registered_date": res["registered"]["date"],
        "phone": res["phone"],
        "picture": res["picture"]["large"]
    }
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)  # Utilise la fonction format_data
            logging.info(f"Data to send: {res}")
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )