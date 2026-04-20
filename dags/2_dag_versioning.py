from airflow.sdk import dag, task 

@dag( 
        dag_id = "second_dag",

)

def second_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    first_task() >> second_task() >> third_task()

second_dag()