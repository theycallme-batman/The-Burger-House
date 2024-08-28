from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
from datetime import datetime

class ADLS_Connect:
    def __init__(self,account_name,sas_token,container):
        account_url = f"https://{account_name}.dfs.core.windows.net"
        self.datalake_service_client = DataLakeServiceClient(account_url, credential=sas_token)
        self.file_system_client = self.datalake_service_client.get_file_system_client(container)   
        
        
    def get_directory(self,topic):
        #Getting epoch
        epoch_time = int(datetime.now().timestamp())
        filepath = f'/{topic}/' + str(epoch_time)
        directory_client = self.file_system_client.create_directory(filepath)         
        directory_client = self.file_system_client.get_directory_client(filepath)
        return directory_client