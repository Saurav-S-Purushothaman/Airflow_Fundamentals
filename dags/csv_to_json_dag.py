import datetime as dt
from datetime import timedelta 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
import pandas as pd

def CSVtoJSON():
    df = pd.read_csv("dags/faker.csv")
    for i, r in df.iterrows():
        print(r["name"])
    df.to_json("dags/fromairflow.json",orient="records")
    
default_args ={
    "owner":"saurav",
    "start_date":dt.datetime(2023,7,1),
    "retries":1, 
    "retry_delay": timedelta(minutes=5)
}

# schedule interval can be set to timedeltas or you can use the crontab format for schedule interval 
# @once 
# @hourly - 0 * * * * 
# @daily - 0 0 * * * 
# @weekly - 0 0 0 * *
# @monthly - 0 0 1 * * 
# @yearly - 0 0 1 1 * 

# crontab format 
# crontab uses the format minute, hour, day of month, month , day of week

# start date variable of dag is start_date + schedule interval 

# create the dag 

with DAG("MYCSVDAG",default_args=default_args,schedule_interval=timedelta(minutes = 5)) as dag: 
    print_starting = BashOperator(
        task_id = "starting", 
        bash_command = "echo 'I am reading the csv now...'"
    )
    CSVJSON = PythonOperator(task_id="convertcsvtojson",
                             python_callable = CSVtoJSON)
    
print_starting >> CSVJSON

