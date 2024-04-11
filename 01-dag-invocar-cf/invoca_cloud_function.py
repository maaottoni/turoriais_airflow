import datetime
import json

import google.auth.transport.requests
from google.cloud import bigquery

from airflow import DAG
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils

from airflow.providers.http.operators.http import (
    SimpleHttpOperator
)

def obter_credenciais_cloud_function(url, nome_funcao):
    """
    Invoca uma Cloud Function no Google Cloud e retorna as credenciais do token de identificação.

    Args:
        url (str): A URL base da Cloud Function.
        nome_funcao (str): O nome da função Cloud.

    Retorna:
        google.auth.transport.requests.IdTokenCredentials: Credenciais do token de identificação.
    """
    url_completa = f"{url}/{nome_funcao}"
    requisicao = google.auth.transport.requests.Request()  
    credenciais_token = id_token_credential_utils.get_default_id_token_credentials(url_completa, request=requisicao) 
    
    return credenciais_token

DAG_ID = "invoca_cloud_function"
HTTP_CONN_ID="http_cloud_function" # Nome da connection do Airflow criada com os dados de host das funções
HOST = "https://us-central1-nome-projeto-123456.cloudfunctions.net" # URL da página das fun~çoes
FUNCTION_ID = "nome_funcao"
TOKEN = obter_credenciais_cloud_function(url=HOST, nome_funcao=FUNCTION_ID)


dag = DAG(
    DAG_ID,
    default_args={
        "retries": 3, "retry_exponential_backoff": True, "retry_delay": 30
    },
    start_date=datetime.datetime(2024, 4, 4),
    catchup=False,
    schedule_interval="0 */12 * * *",
)

task_invoca_cloud_function = SimpleHttpOperator(
            task_id= "invoca_funcao_01",
            method="POST",
            http_conn_id='http_cloud_function',
            endpoint=FUNCTION_ID,
            data=json.dumps({"chave": "valor"}), # parametros necessarios para a função caso haja 
            headers={'Authorization': f"bearer {TOKEN}", "Content-Type": "application/json"},
    )

task_invoca_cloud_function