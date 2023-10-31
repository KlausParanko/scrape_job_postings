from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

container_name = "list-of-postings"
storage_account_name = "klausparjobpostings"
blob_client = BlobServiceClient(
    r"https://klausparjobpostings.blob.core.windows.net/list-of-postings?sp=racwl&st=2023-10-31T11:31:17Z&se=2025-10-31T19:31:17Z&sv=2022-11-02&sr=c&sig=pQaVZuQWR1VyDgRPxyouMhLX6Dk9Jh676pGIbx0txEM%3D",
)
key = r"I+H1TUCXQC/bvhq+oUGgHovpTUZtl6bgqfLuKZtF0TOzDGQA1RLkAEuLn16flvUxdoqoEqxoDqef+AStrnCBtQ=="
