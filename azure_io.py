def upload_file_to_blob(
    blob_service_client,
    azure_filename,
    local_filepath,
):
    container_client = blob_service_client.get_container_client(container=".")
    with open(local_filepath, mode="rb") as data:
        container_client.upload_blob(name=azure_filename, data=data, overwrite=True)


def download_blob_to_file(
    blob_service_client,
    local_path,
    azure_filename,
):
    blob_client = blob_service_client.get_blob_client(
        container=".", blob=azure_filename
    )

    with open(local_path, mode="wb") as blob:
        download_stream = blob_client.download_blob()
        blob.write(download_stream.readall())
