from airflow.sdk import dag, task 

@dag( 
        dag_id = "xcoms_dag_manual",

)

def xcoms_dag_manual():

    @task.python
    def first_task(**kwargs):

        ti = kwargs['ti']
        fetched_data = {"data": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value=fetched_data)
 
    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')['data']
        transformed_data = fetched_data * 2
        transformed_data_dic = {"transf_data": transformed_data}
        ti.xcom_push(key='return_result', value=transformed_data_dic)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids = 'second_task', key = 'return_result')
        return load_data
    

    first = first_task()
    second = second_task()
    third = third_task()


    first >> second >> third

xcoms_dag_manual()