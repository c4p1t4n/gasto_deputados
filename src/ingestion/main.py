import requests
import awswrangler as wr
import pandas as pd
from datetime import datetime
import logging
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
        response = requests.get(url, params=params)
        response.raise_for_status()
    
        data = response.json().get('dados',[])
        if not data:
            logging.error("No data found")
            return 
        return data[0].get('id')
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except ValueError as e:
        logging.error(f"Error processing JSON response: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def get_current_deputados(id):
    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

    params = {
        'idLegislatura': id,
        'ordem': 'ASC',
        'ordenarPor': 'nome'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    
        data = response.json()
        if not data:
            logging.error("No data found")
            return 
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except ValueError as e:
        logging.error(f"Error processing JSON response: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def get_despesas_per_deputado(id_deputado:str):
    url = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_deputado}/despesas"
    params = {
        'ordem':'DESC',
        'ordenarPor': 'ano'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    
        data = response.json()
        if not data:
            logging.error("No data found")
            return 
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except ValueError as e:
        logging.error(f"Error processing JSON response: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def lambda_handler(event,context):
    current_legislatura_id = get_current_legislatura()



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