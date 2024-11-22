from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests

# Function to be executed by the PythonOperator
def print_task(task_name):
 print(f"Executing task: {task_name}")
 
def fetch_people_from_api():
   api_url = "http://api.open-notify.org/astros.json"
   response = requests.get(api_url)
   if response.status_code == 200:
    return response.json() # Returns a list of posts
   else:
    raise ValueError("Failed to fetch data from the API")
people = fetch_people_from_api()
task_names = []
for i in range(people['number']):
 p = people['people'][i]['name'].split(" ")[0]
#title = post['title']
 if p not in task_names:
  task_names.append(p)

# List of task names
#task_names = ['task_1', 'task_2', 'task_3', 'task_4']

# Define the DAG
dag = DAG(
'dynamic_task_example',
 description='A simple dynamic DAG',
 schedule_interval=None, # Define the schedule, 'None' for manual trigger
 start_date=datetime(2024, 11, 21),
 catchup=False,
)

# Create a start task
start_task = DummyOperator(
 task_id='start',
 dag=dag,
)

# Dynamically generate tasks based on task_names
for task_name in task_names:
 task = PythonOperator(
 task_id=task_name,
 python_callable=print_task,
 op_args=[task_name],
 dag=dag,
)

# Set task dependencies: start -> task -> end
start_task >> task
