from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import mimetypes
import os
from google.cloud import storage
import traceback


JSON_DICT = {
  "type": os.environ['type'],
  "project_id": os.environ['project_id'],
  "private_key_id": os.environ['private_key_id'],
  "private_key": os.environ['private_key'].replace('\\n', '\n'),
  "client_email": os.environ['client_email'],
  "client_id": os.environ['client_id'].replace(',', ''),
  "auth_uri": os.environ['auth_uri'],
  "token_uri": os.environ['token_uri'],
  "auth_provider_x509_cert_url": os.environ['auth_provider_x509_cert_url'],
  "client_x509_cert_url": os.environ['client_x509_cert_url'],
  "universe_domain": os.environ['universe_domain']
}

PATH_UC = os.environ['id_path']
SCOPES=["https://www.googleapis.com/auth/drive"]


def download_gsc(bucket, key):    
    print(f"Downloading key: {key} in bucket: {bucket}")
    client = storage.Client.from_service_account_info(JSON_DICT)
    
    source_bucket = client.bucket(bucket)
    blob = source_bucket.blob(key)
    
    tmpdir = "/tmp/file"
    blob.download_to_filename(tmpdir)
    
    return tmpdir


def delete_files(service):
    results = (
        service
        .files()
        .list(
            fields='files(id, name)'
        )
        .execute()
    )

    files = results.get('files', [])

    for file in files:
        if file['name'] != 'ultima_chance':
            service.files().delete(fileId=file['id']).execute()
            print(f"Deleted {file['name']} (ID: {file['id']})")


def move_gcs_data(
    local_path: str,
    remote_path: str
):

    creds = service_account.Credentials.from_service_account_info(
        info=JSON_DICT,
        scopes=SCOPES
    )

    try:
        name = os.path.basename(remote_path)
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {"name": name, "parents": [PATH_UC]}

        delete_files(service)

        mimetype, __ = mimetypes.guess_type(local_path)
        media = MediaFileUpload(
            local_path, 
            mimetype=mimetype, 
            resumable=True,
            chunksize=250 * 1024 * 1024
        )
        
        file = (
            service.files() 
            .create(
                body=file_metadata, 
                media_body=media
            )
        )
        
        response = None
        while response is None:
            status, response = file.next_chunk()
            if status:
                print(f"Uploaded {int(status.progress() * 100):02d} %")

        print(f'As Completed .. {name} 100 %')

    except HttpError as e:
        print(traceback.format_exc())


def transfer_database_uc(event, context):
    bucket = event['bucket']
    key = event['name']

    if key != 'ULTIMA_CHANCE/data.duckdb':
        print(f'[NOT COPY] Key -- {key}, True: ULTIMA_CHANCE/data.duckdb')
        return

    try:

        local_path = download_gsc(bucket, key)
        move_gcs_data(local_path, key)

    except Exception as e:
        print(traceback.format_exc())