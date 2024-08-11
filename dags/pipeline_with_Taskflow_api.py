'''
Here the DAG with Taskflow_API do the following tasks:

Task 1: Start with an initial number (Eg: 10)
Task 2: Add 5 to the number
Task 3: Multiply the updated number by 3
Task 4: Subtract 3 from the updated number
Task 5: Compute the square of the updated number
'''

"""
Taskflow API by Airflow

Taskflow API is a new way to define workflows in Apache Airflow. 
It is a more Pythonic way to define workflows compared to the traditional way of using operators. 
The Taskflow API is built on top of the XCom system, which allows tasks to exchange data between them.
"""

# Importiang Important Libraries

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG 

with DAG(dag_id="MathOperationPipeline_with_taskflow", start_date=datetime(2021, 1, 1), schedule_interval="@once") as dag:

    # Task 1
    @task
    def start_number():
        initial_value = 10
        print(f"Initial number: {initial_value}")
        return initial_value
    
    # Task 2
    @task
    def add_five(current_value):
        updated_value = current_value + 5
        print(f"Updated number after adding 5: {updated_value}")
        return updated_value
    
    # Task 3
    @task
    def multiple_by_three(current_value):
        updated_value = current_value * 3
        print(f"Updated number after multiplying by 3: {updated_value}")
        return updated_value
    
    # Task 4
    @task
    def subtract_three(current_value):
        updated_value = current_value - 3
        print(f"Updated number after subtracting 3: {updated_value}")
        return updated_value
    
    # Task 5
    @task
    def square_number(current_value):
        updated_value = current_value ** 2
        print(f"Final number after squaring: {updated_value}")
        return updated_value

    # Define the task dependencies
    start_number_task = start_number()
    add_five_task = add_five(start_number_task)
    multiple_by_three_task = multiple_by_three(add_five_task)
    subtract_three_task = subtract_three(multiple_by_three_task)
    square_number_task = square_number(subtract_three_task)

    # Benefits of Taskflow API
    # 1. Taskflow API is a more Pythonic way to define workflows compared to the traditional way of using operators.
    # 2. Taskflow API is built on top of the XCom system, which allows tasks to exchange data between them.
    # 3. Taskflow API provides better support for branching and conditional execution of tasks.
    # 4. Taskflow API provides better support for error handling and retries.
    # 5. Taskflow API provides better support for scheduling and triggering of tasks.
