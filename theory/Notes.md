# Apache Airflow Concepts with Code Examples

In **Apache Airflow**, a **Directed Acyclic Graph (DAG)** is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies.

A DAG is defined in a Python script, which represents the workflow structure of the tasks.

### 1. DAG Definition

A **DAG** organizes tasks in a workflow. Below is an example of defining a simple DAG in Airflow.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the DAG
with DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    pass
```

### 2. Tasks

A **task** represents a unit of work in a workflow. It can be anything like running a Python function, executing a SQL query, or running a shell script.

Example of a simple task that prints "Hello World" using a **PythonOperator**:

```python
def print_hello():
    print("Hello World")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)
```

### 3. Operators

An **Operator** defines the type of task that needs to be executed. Operators are the building blocks of tasks in Airflow.

- **PythonOperator**: Runs a Python function.
- **BashOperator**: Executes bash commands.
- **SQLOperator**: Executes SQL queries.

#### PythonOperator Example

```python
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print("Hello from PythonOperator!")

python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_hello,
    dag=dag
)
```

#### BashOperator Example

```python
from airflow.operators.bash_operator import BashOperator

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from BashOperator!"',
    dag=dag
)
```

#### SQL Operator Example

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

sql_task = PostgresOperator(
    task_id='run_sql',
    postgres_conn_id='my_postgres_conn',
    sql="SELECT * FROM my_table;",
    dag=dag
)
```

### 4. Task Instance

A **Task Instance** is a specific run of a task in a DAG. Every time a DAG is executed, task instances are created for each task in the DAG.

- To view task instance details, you can access the Airflow UI.

Example of viewing task instance details in the logs:

```python
from airflow.operators.dummy_operator import DummyOperator

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag
)

# Run the task
dummy_task.run(start_date=days_ago(1), end_date=days_ago(1))
```

### 5. Task Dependency

A **Task Dependency** defines the order in which tasks should be executed. For example, Task B can depend on Task A, meaning Task B will only run after Task A has successfully completed.

```python
# Task dependencies
hello_task >> bash_task  # bash_task will only run after hello_task is completed
```

This sets a dependency where `bash_task` will only execute after `hello_task`.

### 6. Context

The **context** is a dictionary containing metadata about the current task instance, such as the task instance ID, execution date, etc. You can access this within Python functions using the `**kwargs` argument.

Example of accessing task context:

```python
def my_task_function(**context):
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    print(f"Task instance: {task_instance}")
    print(f"Execution date: {execution_date}")

context_task = PythonOperator(
    task_id='context_task',
    python_callable=my_task_function,
    provide_context=True,
    dag=dag
)
```

### 7. XCom

**XCom** (cross-communication) is a way for tasks to share data between them. A task can push some data to XCom, and another task can pull that data from XCom.

- **Pushing Data to XCom**: A task pushes data using `xcom_push`.
- **Pulling Data from XCom**: Another task pulls data using `xcom_pull`.

#### Example of XCom Push:

```python
def push_data(**kwargs):
    kwargs['task_instance'].xcom_push(key='message', value='Hello from Task A')

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_data,
    provide_context=True,
    dag=dag
)
```

#### Example of XCom Pull:

```python
def pull_data(**kwargs):
    message = kwargs['task_instance'].xcom_pull(task_ids='push_task', key='message')
    print(f"Received message: {message}")

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_data,
    provide_context=True,
    dag=dag
)

# Set dependencies
push_task >> pull_task
```

In this example, `push_task` pushes data to XCom, and `pull_task` pulls the data and prints it.

---

These are the foundational concepts of Apache Airflow. Each part of a workflow—tasks, dependencies, context, and communication—can be customized to suit your pipeline's needs.