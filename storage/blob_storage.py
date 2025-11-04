"""
Azure Blob Storage client for handling blob operations.
"""
import os
from typing import Optional, Union
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


class AzureBlobStorage:
    """Client for Azure Blob Storage operations."""

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize Azure Blob Storage client.

        Args:
            connection_string: Azure Storage connection string.
                             If not provided, will read from AZURE_STORAGE_CONNECTION_STRING env var.
        """
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("Azure Storage connection string is required")

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def create_container(self, container_name: str) -> ContainerClient:
        """
        Create a container in the storage account.

        Args:
            container_name: Name of the container to create.

        Returns:
            ContainerClient for the created container.

        Raises:
            ResourceExistsError: If container already exists.
        """
        try:
            container_client = self.blob_service_client.create_container(container_name)
            return container_client
        except ResourceExistsError:
            # Container already exists, return the existing one
            return self.blob_service_client.get_container_client(container_name)

    def upload_blob(
        self,
        container_name: str,
        blob_name: str,
        data: Union[bytes, str],
        overwrite: bool = True
    ) -> BlobClient:
        """
        Upload a blob to a container.

        Args:
            container_name: Name of the container.
            blob_name: Name of the blob.
            data: Data to upload (bytes or string).
            overwrite: Whether to overwrite if blob exists.

        Returns:
            BlobClient for the uploaded blob.
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        # Convert string to bytes if needed
        if isinstance(data, str):
            data = data.encode('utf-8')

        blob_client.upload_blob(data, overwrite=overwrite)
        return blob_client

    def download_blob(self, container_name: str, blob_name: str) -> bytes:
        """
        Download a blob from a container.

        Args:
            container_name: Name of the container.
            blob_name: Name of the blob.

        Returns:
            Blob data as bytes.

        Raises:
            ResourceNotFoundError: If blob doesn't exist.
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        download_stream = blob_client.download_blob()
        return download_stream.readall()

    def get_blob_as_text(self, container_name: str, blob_name: str, encoding: str = 'utf-8') -> str:
        """
        Download a blob and return as text.

        Args:
            container_name: Name of the container.
            blob_name: Name of the blob.
            encoding: Text encoding to use (default: utf-8).

        Returns:
            Blob data as string.
        """
        data = self.download_blob(container_name, blob_name)
        return data.decode(encoding)

    def blob_exists(self, container_name: str, blob_name: str) -> bool:
        """
        Check if a blob exists.

        Args:
            container_name: Name of the container.
            blob_name: Name of the blob.

        Returns:
            True if blob exists, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def delete_blob(self, container_name: str, blob_name: str) -> None:
        """
        Delete a blob from a container.

        Args:
            container_name: Name of the container.
            blob_name: Name of the blob.
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        blob_client.delete_blob()

    def delete_container(self, container_name: str) -> None:
        """
        Delete a container and all its blobs.

        Args:
            container_name: Name of the container to delete.
        """
        self.blob_service_client.delete_container(container_name)

    def list_blobs(self, container_name: str) -> list:
        """
        List all blobs in a container.

        Args:
            container_name: Name of the container.

        Returns:
            List of blob names.
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        return [blob.name for blob in container_client.list_blobs()]
