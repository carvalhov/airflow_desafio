# 1. Instalação do Airflow

Nessa etapa do desafio foi necessário realizar a instalação do airflow através do wsl (ubunto), o processo foi realizado seguindo o script <install_airflow_script.sh> 

No terminal wsl ubunto, inicialmente, realizou-se o comando: 

> chmod +x install_airflow_script.sh

O objetivo, é tornar o arquivo executável. 

Em seguida, realiza-se o segundo comando para exercutar o script:

> ./install_airflow_script.sh

# 2. Login no airflow (host 8080)

Depois do processo de instalação, verificasse se já é possível logar no host local 'http://localhost:8080/' e se a pasta airflow-data foi criada corretamente. 
A pasta deve conter os seguintes arquivos (com excessão da pasta 'dags' que deve ser criada): 
![image](https://github.com/user-attachments/assets/ab5d03e7-c1d3-451e-bcab-41f167e8b425)

# 3. Criação de dags do desafio

Na pasta 'dags' criada, foi criado o arquivo 'northwind_to_csv.py' (usando o arquivo examplo_desafio.py como base), que possui as dags para a realização do desafio.

```python
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
        python_callable=export_final_output
    )

    # Definir a ordem de execução das tasks
    task1 >> task2 >> export_final_output
```
## 3.1. Gerar arquivo `output_orders.csv` da tabela 'Orders' do arquivo data/Northwind_small.sqlite

```python
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
```
## 3.2. Gerar arquivo count.txt 
Gera a soma da quantidade de orders com destino ao 'Rio de Janeiro' e cria a o arquivo count.txt com o resultado

```python
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
```

## 3.3. Criar arquivo 'export_final_output'

```python
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
```

# 4. Interface do Airflow
Interface onde as tasks foram sendo testadas e corrigidas:
![image](https://github.com/user-attachments/assets/0bd46ec1-77a9-42d9-bc35-a4f3278d4396)

# 5. Pontos para destacar

+ O airflow não funciona de forma direta no windows, portanto, deve ser usado através do linux (pode ser um terminal wsl, com o ubunto instalado) ou usar através do docker com o bash.
+ Como foi utilizado o wsl nesse desafio, todos os caminhos para arquivos devem ser no modelo do linux (/mnt/c/Users/) e não no formato do windows, tive problemas nessa parte.
+ 


