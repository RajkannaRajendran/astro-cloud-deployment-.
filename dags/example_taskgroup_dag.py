from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}
dag = DAG(
    'example_taskgroup_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)
start = DummyOperator(task_id='start', dag=dag)
with TaskGroup('group1', dag=dag) as group1:
    task1 = DummyOperator(task_id='task1')
    task2 = DummyOperator(task_id='task2')
with TaskGroup('group2', dag=dag) as group2:
    task3 = DummyOperator(task_id='task3')
    task4 = DummyOperator(task_id='task4')
end = DummyOperator(task_id='end', dag=dag)
start >> group1 >> group2 >> end