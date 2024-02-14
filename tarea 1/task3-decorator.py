from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow'
}

@dag(
    default_args=default_args, 
    catchup=False, 
    description='DAG para crear y exportar DataFrame con @task',
    start_date=datetime(2024,2,3),
    schedule_interval="@once"
)

def tarea_crear_y_exportar_dataframe():
    
    @task
    def crear_dataframe():
        # Crear un DataFrame simple con pandas
        df = pd.DataFrame({
            'Columna1': [1, 2, 3, 4, 5],
            'Columna2': ['a', 'b', 'c', 'd', 'e']
        })
        print("DataFrame creado:")
        print(df)
        return df
    
    @task
    def exportar_dataframe(df: pd.DataFrame):
        # Exportar el DataFrame a un archivo CSV
        filepath = '/tmp/dataframe_simple.csv'
        df.to_csv(filepath, index=False)
        print(f"DataFrame exportado como CSV a {filepath}")

    # Definir el flujo de las tareas
    df = crear_dataframe()
    exportar_dataframe(df)

# Asignar el DAG a una variable para Airflow lo detecte
dag = tarea_crear_y_exportar_dataframe()
