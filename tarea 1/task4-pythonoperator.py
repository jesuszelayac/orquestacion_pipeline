from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import numpy as np

def ordenar_lista() :
    #Crear lista desordenada con numpy
    lista_desordenada = np.random.rand(10)
    print("Lista desordenada : ", lista_desordenada)

    #Ordenar la lista
    lista_ordenada = np.sort(lista_desordenada)
    print("Lista ordernada : ", lista_ordenada)

with DAG(dag_id="task4_pythonoperator",
         description="Con numpy crear una lista desordenada y utilizar sort para ordenar de forma ascedente",
         start_date=datetime(2024,2,13),
         schedule_interval="@once"
         ) as dag:
    
    t1 = PythonOperator(task_id="ordenar_lista_con_numpy",
                        python_callable=ordenar_lista)