from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import mimetypes
import os
from google.cloud import storage
import traceback
from secret import access_secret_version
import json


SECRET_ID = os.environ['secret_id']
PROJECT_IC = os.environ['project_id']
PATH_UC = os.environ['id_path']
SCOPES=["https://www.googleapis.com/auth/drive"]


JSON_DICT = json.loads(
    access_secret_version(
        project_id=PROJECT_IC,
        secret_id=SECRET_ID,
        version_id=1
    )
)


def download_gcs(
    bucket: str, 
    key: str
) -> str:
        
    print(f"Downloading key: {key} in bucket: {bucket}")
    client = storage.Client.from_service_account_info(JSON_DICT)
    
    source_bucket = client.bucket(bucket)
    blob = source_bucket.blob(key)
    
    tmpdir = "/tmp/file"
    blob.download_to_filename(tmpdir)
    
    return tmpdir


def delete_files(service, name):
    results = (
        service
        .files()
        .list(
            q=f"name = '{name}'",
            fields='files(id, name)'
        )
        .execute()
    )

    files = results.get('files', [])

    for file in files:
        service.files().delete(fileId=file['id']).execute()
        print(f"Deleted {file['name']} (ID: {file['id']})")


def move_gcs_data(
    local_path: str,
    remote_path: str
) -> None:

    creds = service_account.Credentials.from_service_account_info(
        info=JSON_DICT,
        scopes=SCOPES
    )

    try:
        name = os.path.basename(remote_path)
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {"name": name, "parents": [PATH_UC]}

        delete_files(service, name)

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

    files = [
        'ULTIMA_CHANCE/data.duckdb', 
        'RESSARCIMENTO/ressarcimento.duckdb'
    ]

    if key not in files:
        print(f'[NOT COPY] Key -- {key}')
        return

    try:

        local_path = download_gcs(bucket, key)
        move_gcs_data(local_path, key)

    except Exception as e:
        print(traceback.format_exc())