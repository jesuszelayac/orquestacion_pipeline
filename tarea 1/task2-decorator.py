from airflow.decorators import dag,task
from datetime import datetime
import numpy as np

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2024, 2, 2)
}

@dag(
    default_args=default_args,
    catchup=False,
    description='Crear una funcion que haga multiplicación de matrices y ejecutarlo con 2 matrices 3x3',
    schedule_interval='@once'
)

def multiplicacion_matrices_dag():
    
    @task
    def multiplicar_matrices():
        # Definimos dos matrices 3x3
        matriz_a = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        matriz_b = np.array([[9, 8, 7], [6, 5, 4], [3, 2, 1]])
        
        # Realizar la multiplicación de matrices
        resultado = np.dot(matriz_a, matriz_b)
        
        print("Resultado de la multiplicación de matrices:")
        print(resultado)

        resultado_lista = resultado.tolist()
        
        return resultado_lista
    
    # Ejecutar la tarea
    multiplicar_matrices()

# Asignar el DAG a una variable para que Airflow lo detecte
dag = multiplicacion_matrices_dag()
