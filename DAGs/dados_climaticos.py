from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from os.path import join
import pandas as pd

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2022, 8, 22),
    schedule_interval='0 0 * * 1', # minuto hora dia_mes mes dia_ou_dia_da_semana 
                                   #   0     0      *     *          1
) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/vitor/Documents/airflowalura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):
        city = 'Boston'
        key = 'CHK6C6QFYHHB2G4FGF4XHXGSP'

        #URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/[location]/[date1]/[date2]?key=YOUR_API_KEY&contentType=csv'
        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
        f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')
        print(URL)

        dados = pd.read_csv(URL)

        file_path = f'/home/vitor/Documents/airflowalura/semana={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2
