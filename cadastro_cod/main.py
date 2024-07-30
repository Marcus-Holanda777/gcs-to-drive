import traceback
from etl import etl_cadastro_uc
import logging

logging.basicConfig(
    format='%(asctime)s %(message)s', 
    datefmt='%d/%m/%Y %H:%M:%S %p',
    level=logging.INFO
)


def gera_cadastro_uc(event, context):
    bucket = event['bucket']
    key = event['name']

    file = 'data.duckdb'

    if key != file:
        return

    try:
        etl_cadastro_uc(bucket, key)
    except Exception as e:
        print(traceback.format_exc())