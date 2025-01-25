import requests
from datetime import datetime
import logging
import pandas as pd

from src.utils.main import upload_s3_parquet_file
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
def get_current_legislatura():
    current_date = datetime.now().strftime('%Y-%m-%d')

    url = "https://dadosabertos.camara.leg.br/api/v2/legislaturas"
    params = {
        'data': current_date,
        'ordem':'DESC',
        'ordenarPor': 'id'
    }
    try:
        response = requests.get(url, params=params,timeout=30)
        response.raise_for_status()
        data_response = response.json().get('dados',[])
        if not data_response:
            logging.error("No data found")
            return 
        return data[0].get('id')
    except requests.exceptions.RequestException as e:
        logging.error("Request error occurred: %s",e)
    except ValueError as e:
        logging.error("Error processing JSON response: %s",e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s",e)


def get_current_deputados(id_deputado:str):
    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

    params = {
        'idLegislatura': id_deputado,
        'ordem': 'ASC',
        'ordenarPor': 'nome'
    }
    try:
        response = requests.get(url, params=params,timeout=30)
        response.raise_for_status()
    
        data_response = response.json()
        if not data_response:
            logging.error("No data found")
            return 
        return data
    except requests.exceptions.RequestException as e:
        logging.error("Request error occurred: %s",e)
    except ValueError as e:
        logging.error("Error processing JSON response: %s",e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s",e)

def get_despesas_per_deputado(id_deputado:str):
    url = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_deputado}/despesas"
    params = {
        'ordem':'DESC',
        'ordenarPor': 'ano'
    }
    try:
        response = requests.get(url, params=params,timeout=30)
        response.raise_for_status()
    
        data_response = response.json()
        if not data_response:
            logging.error("No data found")
            return 
        return data
    except requests.exceptions.RequestException as e:
        logging.error("Request error occurred: %s",e)
    except ValueError as e:
        logging.error("Error processing JSON response: %s",e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s",e)


def lambda_handler(event,context):
    pass



current_legislatura_id = get_current_legislatura()
data =  get_current_deputados(current_legislatura_id)
df = pd.DataFrame.from_dict(data['dados'])
df_spending = pd.DataFrame()
for x in df.id.to_list():
    data_spending =  get_despesas_per_deputado(x)
    temp_df = pd.DataFrame.from_dict(data_spending['dados'])
    temp_df['id'] = x
    df_spending = pd.concat([df_spending,temp_df],ignore_index=True)

upload_s3_parquet_file(df=df,path='s3://gastos-deputados-9723-dev/deputados/')
upload_s3_parquet_file(df=df_spending,path='s3://gastos-deputados-9723-dev/gastos/')