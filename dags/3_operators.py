from airflow.sdk import dag, task 
from airflow.operators.bash import BashOperator

@dag( 
        dag_id = "bash_dag",

)

def bash_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def bash_task_mordern():
        return "echo https://airflow,apache.org/"

    bash_task_oldschool = BashOperator(
        task_id = "bash_task_oldschool",
        bash_command = "echo https://airflow,apache.org/" )


    first_task() >> second_task() >> bash_task_mordern() >> bash_task_oldschool

bash_dag()