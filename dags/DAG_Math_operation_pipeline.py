'''
Here the DAG do the following tasks:

Task 1: Start with an initial number (Eg: 10)
Task 2: Add 5 to the number
Task 3: Multiply the updated number by 3
Task 4: Subtract 3 from the updated number
Task 5: Compute the square of the updated number

'''

# Import the required libraries

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the DAG Tasks

# Task 1
def start_number(**context): 
    context['task_instance'].xcom_push(key='current_value', value=10) # Push the initial number to XCom
    print(f"Initial number: {10}")

# Task 2
def add_five(**context):
    current_value = context['task_instance'].xcom_pull(key='current_value', task_ids='start_number') # Pull the initial number from XCom
    updated_value = current_value + 5
    context['task_instance'].xcom_push(key='current_value', value=updated_value) # Push the updated number to XCom
    print(f"Updated number after adding 5: {updated_value}")

# Task 3
def multiple_by_three(**context):
    current_value= context['task_instance'].xcom_pull(key='current_value', task_ids='add_five') # Pull the updated number from XCom
    updated_value = current_value * 3
    context['task_instance'].xcom_push(key='current_value', value=updated_value) # Push the updated number to XCom
    print(f"Updated number after multiplying by 3: {updated_value}")

# Task 4
def subtract_three(**context):
    current_value = context['task_instance'].xcom_pull(key='current_value', task_ids='multiple_by_three') # Pull the updated number from XCom
    updated_value = current_value - 3
    context['task_instance'].xcom_push(key='current_value', value=updated_value) # Push the updated number to XCom
    print(f"Updated number after subtracting 3: {updated_value}")

# Task 5
def square_number(**context):
    current_value = context['task_instance'].xcom_pull(key='current_value', task_ids='subtract_three') # Pull the updated number from XCom
    updated_value = current_value ** 2
    print(f"Final number after squaring: {updated_value}")

# Define the DAG
with DAG(dag_id="MathOperationPipeline", start_date=datetime(2021, 1, 1), schedule_interval="@daily") as dag:

    start_number = PythonOperator(task_id="start_number", python_callable=start_number)
    add_five = PythonOperator(task_id="add_five", python_callable=add_five)
    multiple_by_three = PythonOperator(task_id="multiple_by_three", python_callable=multiple_by_three)
    subtract_three = PythonOperator(task_id="subtract_three", python_callable=subtract_three)
    square_number = PythonOperator(task_id="square_number", python_callable=square_number)

    # Define the task dependencies
    start_number >> add_five >> multiple_by_three >> subtract_three >> square_number

# Run the DAG