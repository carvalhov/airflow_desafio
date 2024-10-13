from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sqlite3
import csv


# Função para ler o arquivo sqlite, selecionadno a tabela 'Order' e gerando o arquivo em csv com os dados da tabela
def export_orders_to_csv():
    conn = sqlite3.connect('/mnt/c/Users/carva/OneDrive/Desktop/Programas/airflow_tooltorial/data/Northwind_small.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM 'Order'")
    rows = cursor.fetchall()
    with open('/mnt/c/Users/carva/OneDrive/Desktop/Programas/airflow_tooltorial/output_orders.csv', mode='w') as file:
        writer = csv.writer(file)
        writer.writerow([i[0] for i in cursor.description]) 
        writer.writerows(rows) 

    conn.close()

def count_quantity_to_rio():
    conn = sqlite3.connect('/mnt/c/Users/carva/OneDrive/Desktop/Programas/airflow_tooltorial/data/Northwind_small.sqlite')
    cursor = conn.cursor()
    
    # Executar a consulta que faz o JOIN entre as tabelas 'OrderDetail' e 'Order'
    cursor.execute("""
        SELECT od.Quantity
        FROM 'OrderDetail' od
        JOIN 'Order' o ON od.OrderID = o.ID
        WHERE o.ShipCity = 'Rio de Janeiro'
    """)
    
    # Calcular a soma das quantidades
    total_quantity = sum(row[0] for row in cursor.fetchall())

    # gerando o arquivo em txt na saída
    with open('/mnt/c/Users/carva/OneDrive/Desktop/Programas/airflow_tooltorial/count.txt', mode='w') as file:
        file.write(str(total_quantity))

    conn.close()

def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None

default_args = {
    'start_date': datetime(2012, 1, 1),
}

with DAG(dag_id='northwind_airflow', 
        default_args=default_args, 
        schedule_interval=timedelta(days=1), catchup=False
) as dag:
    
    # Task 1: Exportar dados da tabela 'Order' para CSV
    task1 = PythonOperator(
        task_id='export_orders_to_csv',
        python_callable=export_orders_to_csv
    )

    # Task 2: Contar a quantidade de produtos enviados para o Rio de Janeiro
    task2 = PythonOperator(
        task_id='count_quantity_to_rio',
        python_callable=count_quantity_to_rio
    )

    # Task final: Exportar o arquivo final
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer
    )

    # Definir a ordem de execução das tasks
    task1 >> task2 >> export_final_output
