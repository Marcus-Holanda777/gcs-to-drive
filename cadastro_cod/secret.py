from google.cloud import secretmanager
import google_crc32c


def access_secret_version(
    project_id: str, 
    secret_id: str,
    version_id: str
) -> secretmanager.AccessSecretVersionResponse:
    
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(request={"name": name})

    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("[ERROR] Service Secret-Key.")
        return response

    payload = response.payload.data.decode("UTF-8")

    return payload