# pip install apache-airflow

# Import the required libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define the DAG Tasks

# Task 1
def preprocess_data():
    print("Preprocessing data...")

# Task 2
def train_model():
    print("Training model...")

# Task 3
def evaluate_model():
    print("Evaluating model...")

## Define the DAG
with DAG(dag_id="MLPipeline", start_date=datetime(2021, 1, 1), schedule_interval="@daily") as dag:
    
    preprocess_data = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data)
    train_model = PythonOperator(task_id="train_model", python_callable=train_model)
    evaluate_model = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)

    # Define the task dependencies
    preprocess_data >> train_model >> evaluate_model

# Run the DAG
