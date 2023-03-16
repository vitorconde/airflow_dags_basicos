import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

# intervalo de datas
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# formatando as datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

city = 'Boston'
key = 'CHK6C6QFYHHB2G4FGF4XHXGSP'

#URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/[location]/[date1]/[date2]?key=YOUR_API_KEY&contentType=csv'
URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')
print(URL)

dados = pd.read_csv(URL)
print(dados.head(5))

file_path = f'/home/vitor/Documents/datapipeline/semana={data_inicio}/'
#os.mkdir(file_path)

dados.to_csv(file_path + 'dados_brutos.csv')
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')


