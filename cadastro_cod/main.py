import traceback
from etl import etl_cadastro_uc


def gera_cadastro_uc(event, context):
    bucket = event['bucket']
    key = event['name']

    files = [
        'ULTIMA_CHANCE/data.duckdb', 
    ]

    if key not in files:
        return

    try:
        etl_cadastro_uc(bucket, key)
    except Exception as e:
        print(traceback.format_exc())