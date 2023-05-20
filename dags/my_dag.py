from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from datetime import datetime 
from random import randint 
from airflow.operators.bash import BashOperator 

# Xcoms - cross communication messages 
# xcoms are used for one task to communicate with other like if we want to get the value returned by 
# one task to be used in another task , then you can use Xcoms 




# need to create the python callable function used in the dag 
def _training_model():
    return randint(1,10)

# we will have to pass task instance object inside the function (ti)
def _choose_best_model(ti):
    # pull the accuracy from the airflow database 
    accuracies = ti.xcom_pull(task_id = [
        "training_model_A"
        "training_model_B"
        "training_model_C"
    ])
    best_accuracy = max(accuracies)
    if best_accuracy > 8 : 
        return "accurate" # task id of accurate 
    return "inaccurate" # task id of inaccurate 


# need to create an instance of dag class 
# use instance manager with to create dag instance
# dag instance requires some parameters 
# dag id each dag will have unique identifier 
# second parameter start date 
# third parameter schedule interval 
# catchup - say airflow to not to catchup the missed dag from startdate to currentdate 

# each time a dag is triggered a dagrun object is created  - instance of your dag running at given date 


with DAG("my_dag",start_date = datetime(2023,1,1), schedule_interval ="@daily",
         catchup = False) as dag: 
    
    # now you can create your task. give suitable name for the task
    # two arguments required 
    # task_id = unique identifier of your task 
    # python_callable function 
    
    training_model_A = PythonOperator(
        task_id = "training_model_A",
        python_callable = _training_model


    )
    training_model_B = PythonOperator(
        task_id = "training_model_B",
        python_callable = _training_model


    )
    training_model_C = PythonOperator(
        task_id = "training_model_C",
        python_callable = _training_model


    )

    choose_best_model = BranchPythonOperator(
        task_id ="choose_best_model",
        python_callable = _choose_best_model
    )
    
    accurate  = BashOperator(
        task_id = "accurate", 
        bach_command = "echo 'accurate'"
    )
    inaccurate  = BashOperator(
        task_id = "inaccurate", 
        bach_command = "echo 'inacurate'"
    )

    [training_model_A,training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]