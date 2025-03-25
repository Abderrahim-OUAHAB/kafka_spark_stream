import csv
import json
import logging
import time
import uuid
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

default_args = {
    'owner': 'wahhab',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def get_random_user_data():
    """
    Récupère des données supplémentaires depuis l'API Random User.
    """
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def get_bank_transactions():
    """
    Lit les transactions bancaires depuis un fichier CSV.
    """
    file_path = "/opt/airflow/dataset/bank_transactions.csv"  # Chemin vers le fichier CSV
    transactions = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            transactions.append(row)
    return transactions

def format_data(transaction, random_user_data):
    """
    Combine les données de transactions bancaires avec les données de l'API Random User.
    """
    data = {
        # Données de transaction bancaire (nécessaires pour le modèle)
        "transaction_id": transaction["TransactionID"],
        "account_id": transaction["AccountID"],
        "transaction_amount": float(transaction["TransactionAmount"]),
        "transaction_date": transaction["TransactionDate"],
        "transaction_type": transaction["TransactionType"],
        "location": transaction["Location"],
        "device_id": transaction["DeviceID"],
        "ip_address": transaction["IP Address"],
        "merchant_id": transaction["MerchantID"],
        "account_balance": float(transaction["AccountBalance"]),
        "previous_transaction_date": transaction["PreviousTransactionDate"],
        "channel": transaction["Channel"],
        "customer_age": int(transaction["CustomerAge"]),
        "customer_occupation": transaction["CustomerOccupation"],
        "transaction_duration": int(transaction["TransactionDuration"]),
        "login_attempts": int(transaction["LoginAttempts"]),

        # Données supplémentaires de l'API Random User (optionnelles)
        "first_name": random_user_data["name"]["first"],
        "last_name": random_user_data["name"]["last"],
        "gender": random_user_data["gender"],
        "picture": random_user_data["picture"]["large"]
    }
    return data

def stream_data():
    """
    Envoie les transactions bancaires combinées avec les données de l'API Random User à Kafka.
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    transactions = get_bank_transactions()
    curr_time = time.time()

    for transaction in transactions:
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:
            random_user_data = get_random_user_data()  # Récupère des données supplémentaires
            formatted_data = format_data(transaction, random_user_data)  # Combine les données
            logging.info(f"Data to send: {formatted_data}")
            producer.send('bank_transactions', json.dumps(formatted_data).encode('utf-8'))
            time.sleep(1)  # Simuler un délai entre les transactions
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

with DAG('bank_transaction_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_bank_transactions',
        python_callable=stream_data
    )