"""
Tests for Azure Blob Storage operations.
"""
import os
import pytest
from dotenv import load_dotenv
from storage.blob_storage import AzureBlobStorage
from azure.core.exceptions import ResourceNotFoundError

# Load environment variables from .env file
load_dotenv()


@pytest.fixture
def blob_storage():
    """Create a blob storage client instance."""
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        pytest.skip("AZURE_STORAGE_CONNECTION_STRING not set")

    return AzureBlobStorage(connection_string)


@pytest.fixture
def test_container_name():
    """Return a unique test container name."""
    import uuid
    return f"test-container-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cleanup_container(blob_storage, test_container_name):
    """Fixture to clean up test container after test."""
    yield
    # Cleanup after test
    try:
        blob_storage.delete_container(test_container_name)
    except Exception:
        pass  # Container might not exist


class TestAzureBlobStorage:
    """Test suite for Azure Blob Storage operations."""

    def test_create_container(self, blob_storage, test_container_name, cleanup_container):
        """Test creating a container."""
        container_client = blob_storage.create_container(test_container_name)

        assert container_client is not None
        assert container_client.container_name == test_container_name

    def test_upload_and_download_blob_text(self, blob_storage, test_container_name, cleanup_container):
        """Test uploading and downloading a text blob."""
        # Create container
        blob_storage.create_container(test_container_name)

        # Test data
        blob_name = "test-file.txt"
        test_data = "Hello, Azure Blob Storage!"

        # Upload blob
        blob_client = blob_storage.upload_blob(test_container_name, blob_name, test_data)
        assert blob_client is not None

        # Download blob
        downloaded_data = blob_storage.get_blob_as_text(test_container_name, blob_name)
        assert downloaded_data == test_data

    def test_upload_and_download_blob_bytes(self, blob_storage, test_container_name, cleanup_container):
        """Test uploading and downloading binary blob data."""
        # Create container
        blob_storage.create_container(test_container_name)

        # Test data
        blob_name = "test-binary.bin"
        test_data = b"Binary data \x00\x01\x02\x03"

        # Upload blob
        blob_storage.upload_blob(test_container_name, blob_name, test_data)

        # Download blob
        downloaded_data = blob_storage.download_blob(test_container_name, blob_name)
        assert downloaded_data == test_data

    def test_blob_exists(self, blob_storage, test_container_name, cleanup_container):
        """Test checking if a blob exists."""
        # Create container
        blob_storage.create_container(test_container_name)

        blob_name = "exists-test.txt"

        # Check non-existent blob
        assert not blob_storage.blob_exists(test_container_name, blob_name)

        # Upload blob
        blob_storage.upload_blob(test_container_name, blob_name, "test data")

        # Check existing blob
        assert blob_storage.blob_exists(test_container_name, blob_name)

    def test_delete_blob(self, blob_storage, test_container_name, cleanup_container):
        """Test deleting a blob."""
        # Create container and upload blob
        blob_storage.create_container(test_container_name)
        blob_name = "delete-test.txt"
        blob_storage.upload_blob(test_container_name, blob_name, "test data")

        # Verify blob exists
        assert blob_storage.blob_exists(test_container_name, blob_name)

        # Delete blob
        blob_storage.delete_blob(test_container_name, blob_name)

        # Verify blob is deleted
        assert not blob_storage.blob_exists(test_container_name, blob_name)

    def test_list_blobs(self, blob_storage, test_container_name, cleanup_container):
        """Test listing blobs in a container."""
        # Create container
        blob_storage.create_container(test_container_name)

        # Upload multiple blobs
        blob_names = ["file1.txt", "file2.txt", "file3.txt"]
        for blob_name in blob_names:
            blob_storage.upload_blob(test_container_name, blob_name, f"Content of {blob_name}")

        # List blobs
        listed_blobs = blob_storage.list_blobs(test_container_name)

        # Verify all blobs are listed
        assert len(listed_blobs) == len(blob_names)
        assert set(listed_blobs) == set(blob_names)

    def test_overwrite_blob(self, blob_storage, test_container_name, cleanup_container):
        """Test overwriting an existing blob."""
        # Create container
        blob_storage.create_container(test_container_name)

        blob_name = "overwrite-test.txt"
        original_data = "Original data"
        new_data = "New data"

        # Upload original blob
        blob_storage.upload_blob(test_container_name, blob_name, original_data)
        assert blob_storage.get_blob_as_text(test_container_name, blob_name) == original_data

        # Overwrite blob
        blob_storage.upload_blob(test_container_name, blob_name, new_data, overwrite=True)
        assert blob_storage.get_blob_as_text(test_container_name, blob_name) == new_data

    def test_download_nonexistent_blob(self, blob_storage, test_container_name, cleanup_container):
        """Test downloading a blob that doesn't exist."""
        # Create container
        blob_storage.create_container(test_container_name)

        # Try to download non-existent blob
        with pytest.raises(ResourceNotFoundError):
            blob_storage.download_blob(test_container_name, "nonexistent.txt")

    def test_basic_create_and_get_workflow(self, blob_storage, test_container_name, cleanup_container):
        """
        Test basic workflow: Create container, upload blob, retrieve blob.
        This is the main test for basic create and get operations.
        """
        # Step 1: Create container
        container_client = blob_storage.create_container(test_container_name)
        assert container_client.container_name == test_container_name
        print(f"✓ Container '{test_container_name}' created successfully")

        # Step 2: Upload a blob
        blob_name = "sample-data.txt"
        test_content = "This is a test file for Azure Blob Storage"

        blob_client = blob_storage.upload_blob(
            container_name=test_container_name,
            blob_name=blob_name,
            data=test_content
        )
        assert blob_client is not None
        print(f"✓ Blob '{blob_name}' uploaded successfully")

        # Step 3: Verify blob exists
        assert blob_storage.blob_exists(test_container_name, blob_name)
        print(f"✓ Blob existence verified")

        # Step 4: Download and verify content
        downloaded_content = blob_storage.get_blob_as_text(test_container_name, blob_name)
        assert downloaded_content == test_content
        print(f"✓ Blob content retrieved and verified successfully")
        print(f"  Original:   '{test_content}'")
        print(f"  Downloaded: '{downloaded_content}'")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
