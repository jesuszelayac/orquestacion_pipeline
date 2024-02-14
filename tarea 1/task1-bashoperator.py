from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(dag_id="task1_bashoperator",
         description="crear carpeta, dentro de la carpeta, crear un archivo .txt que contenga: Mi primera DAG",
         start_date = datetime(2024,2,13),
         schedule_interval="@once") as dag:
    
    t1 = BashOperator(task_id="create_folder_bash",
                      bash_command="""
                      echo "Mi primera DAG" > prueba.txt
                      """)