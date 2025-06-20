'''
Test the upload function that is used by the sync agent.
The idea is to make sure to test for various possible failures and to ensure that it is not able to upload corrupted files.

To test:
* providing everything correctly should succeed
* providing a wrong checksum should give a uploadFailedException --> file status should not be secured ## check that the put function is called 3 times.
* providing a wrong file (e.g. a text file with the last two character flipped) --> file status should not be secured
* if the put command inserts a file, that has a longer/shorter length, the upload should also fail.
* if the put command returns a connection error, the upload should also fail (e.g. urllib3.exceptions.ReadTimeoutError)
* let the put command return in the headers an content-md5, etag and nothing -> provide a false and correct checksum, check that the function complets only for the correct cases.
* let the put command return a code 404
* test a scenario where the put command succeeds in try 3 (e.g. upload of all the data)
* test what happens if a zero bytes file is uploaded --> should succeed.

'''

import uuid
import datetime
import tempfile
import os
import json
import pytest
import base64
import hashlib
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import ReadTimeout, JSONDecodeError
import urllib3.exceptions
import xarray as xr
import numpy as np

from etiket_client.local.models.file import FileSelect
from etiket_client.remote.endpoints.dataset import dataset_create
from etiket_client.remote.endpoints.file import file_create, file_generate_presigned_upload_link_single, file_read
from etiket_client.remote.endpoints.models.dataset import DatasetCreate as DatasetCreateRemote
from etiket_client.remote.endpoints.models.file import FileCreate as FileCreateRemote, FileSignedUploadLink
from etiket_client.exceptions import uploadFailedException
from etiket_client.remote.endpoints.models.types import FileStatusRem, FileType
from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single, MAX_TRIES
from etiket_sync_agent.sync.checksums.any import md5
from etiket_sync_agent.sync.checksums.hdf5 import md5_netcdf4


def create_test_file(temp_dir: str, file_name: str, is_empty: bool = False, content: dict = None) -> str:
    """
    Generic function to create test files for testing.
    
    Args:
        temp_dir: Temporary directory path
        file_name: Name of the file to create
    
    Returns:
        Path to the created file
    """
    file_path = os.path.join(temp_dir, f"{file_name}.json")
    if is_empty:
        # Create empty JSON file
        with open(file_path, 'w') as f:
            pass
    else:
        if content is None:
            content = {
                "test": "data",
                "timestamp": str(datetime.datetime.now()),
                "uuid": str(uuid.uuid4())
            }
        with open(file_path, 'w') as f:
            json.dump(content, f)
    
    return file_path

def create_upload_info(temp_dir, scope_uuid, content='aaaaaaaaaAAAaaaaa') -> FileSignedUploadLink:
    """Create a mock FileSignedUploadLink for testing."""
    ds_uuid = uuid.uuid4()
    file_uuid = uuid.uuid4()
    file_name = "test"
    
    file_path = create_test_file(temp_dir, file_name, is_empty=False, content=content)
    version_id = 10000

    remote_dataset_create = DatasetCreateRemote(
            uuid=ds_uuid,
            name=f"Test Dataset",
            description="Test dataset",
            keywords=["test"],
            attributes={},
            ranking=0,
            creator="test_user",
            scope_uuid=scope_uuid,
            collected=datetime.datetime.now()
        )
    dataset_create(remote_dataset_create)

    file_create_remote = FileCreateRemote(
        name=file_name,
        filename=os.path.basename(file_path),
        uuid=file_uuid,
        creator="test_user",
        collected=datetime.datetime.now(),
        size=os.path.getsize(file_path),
        type=FileType.JSON,
        file_generator="test",
        version_id=version_id,
        ds_uuid=ds_uuid,
        immutable=False
    )
    
    file_create(file_create_remote)
    
    upload_info = file_generate_presigned_upload_link_single(file_create_remote.uuid, version_id)
    md5_hash = md5(file_path)
    md5_hash_netcdf4 = None
    if file_path.endswith('.hdf5'):
        md5_hash_netcdf4 = md5_netcdf4(file_path)
    return file_path, upload_info, md5_hash, md5_hash_netcdf4

def create_wrong_checksum() -> any:
    """Create an intentionally wrong MD5 checksum for testing failures."""
    # Create a fake checksum that won't match the file
    wrong_hash = hashlib.md5(b"wrong_content")
    return wrong_hash

class TestUploadFunction:
    """Test suite for the upload_new_file_single function."""

    @pytest.fixture
    def temp_dir(self):
        """Fixture to create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    def test_upload_success_json_file(self, temp_dir, get_scope_uuid):
        """Test successful upload of a JSON file."""
        # Create test file
        scope_uuid = get_scope_uuid
    
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        upload_new_file_single(file_path, upload_info, md5_checksum)
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured
        assert r_files[0].md5_checksum == str(md5_checksum.hexdigest())

    def test_upload_wrong_checksum(self, temp_dir, get_scope_uuid):
        """Test upload of a file with a wrong checksum."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        wrong_checksum = create_wrong_checksum()

        with pytest.raises(uploadFailedException):
            upload_new_file_single(file_path, upload_info, wrong_checksum)
            
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending
    
    def test_upload_wrong_file(self, temp_dir, get_scope_uuid):
        """Test upload of a file where server receives different content than expected."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        # Create a different file with wrong content to simulate what server would receive
        wrong_file_path = create_test_file(temp_dir, "wrong_file", content={"wrong": "content", "corrupted": True})
        
        from etiket_sync_agent.sync.uploader.file_uploader import client
        original_put = client.session.put
        
        def mock_put_with_wrong_etag(url, data, timeout, headers):
            """Mock that simulates server receiving different content by returning wrong ETag"""
            with open(wrong_file_path, 'rb') as wrong_file:
                response = original_put(url, wrong_file, timeout=timeout, headers=headers)
                return response
        
        # Mock the put request to simulate wrong content upload
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_with_wrong_etag) as mock_put:
            # The upload should fail due to checksum mismatch
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            # Verify the put method was called the maximum number of times (3 retries)
            assert mock_put.call_count == MAX_TRIES
        
        # Verify that the file status remains pending due to failed upload
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_file_length_mismatch(self, temp_dir, get_scope_uuid):
        """Test upload failure when server receives file with different length."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid, content='aaaaaaaaaAAAaaaaa')
        longer_file_path = create_test_file(temp_dir, "longer_file", content='aaaaaaaaaAAAaaaaaaaaa')
        
        from etiket_sync_agent.sync.uploader.file_uploader import client
        original_put = client.session.put
        
        def mock_put_different_length(url, data, timeout, headers):
            """Mock that simulates server receiving file with different length"""
            # Create a mock response that indicates different file size was received
            with open(longer_file_path, 'rb') as file:
                response = original_put(url, file, timeout=timeout, headers=headers)
                return response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_different_length) as mock_put:
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            assert mock_put.call_count == MAX_TRIES
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_connection_error(self, temp_dir, get_scope_uuid):
        """Test upload failure when connection error occurs."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_timeout(*args, **kwargs):
            """Mock that raises ReadTimeoutError"""
            raise urllib3.exceptions.ReadTimeoutError(None, None, "Read timeout")
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_timeout) as mock_put:
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            assert mock_put.call_count == MAX_TRIES
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_content_md5_header_correct(self, temp_dir, get_scope_uuid):
        """Test upload with correct Content-MD5 header verification."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_content_md5_correct(url, data, timeout, headers):
            """Mock that returns correct Content-MD5 header"""
            mock_response = Mock()
            mock_response.status_code = 200
            # Return the correct base64-encoded MD5
            correct_md5_base64 = base64.b64encode(md5_checksum.digest()).decode('utf-8')
            mock_response.headers = {'Content-MD5': correct_md5_base64}
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_content_md5_correct):
            upload_new_file_single(file_path, upload_info, md5_checksum)
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured

    def test_upload_content_md5_header_incorrect(self, temp_dir, get_scope_uuid):
        """Test upload with incorrect Content-MD5 header verification."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_content_md5_incorrect(url, data, timeout, headers):
            """Mock that returns incorrect Content-MD5 header"""
            mock_response = Mock()
            mock_response.status_code = 200
            # Return a wrong MD5 checksum
            wrong_md5_base64 = base64.b64encode(b"wrong_checksum_data").decode('utf-8')
            mock_response.headers = {'Content-MD5': wrong_md5_base64}
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_content_md5_incorrect) as mock_put:
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            assert mock_put.call_count == MAX_TRIES
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_etag_header_correct(self, temp_dir, get_scope_uuid):
        """Test upload with correct ETag header verification."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_etag_correct(url, data, timeout, headers):
            """Mock that returns correct ETag header"""
            mock_response = Mock()
            mock_response.status_code = 200
            # Return the correct MD5 as ETag (with quotes as is common)
            correct_etag = f'"{md5_checksum.hexdigest()}"'
            mock_response.headers = {'ETag': correct_etag}
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_etag_correct):
            upload_new_file_single(file_path, upload_info, md5_checksum)
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured

    def test_upload_etag_header_incorrect(self, temp_dir, get_scope_uuid):
        """Test upload with incorrect ETag header verification."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_etag_incorrect(url, data, timeout, headers):
            """Mock that returns incorrect ETag header"""
            mock_response = Mock()
            mock_response.status_code = 200
            # Return a wrong ETag
            mock_response.headers = {'ETag': '"wrong_etag_value"'}
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_etag_incorrect) as mock_put:
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            assert mock_put.call_count == MAX_TRIES
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_no_verification_headers(self, temp_dir, get_scope_uuid):
        """Test upload when no verification headers (Content-MD5 or ETag) are returned."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_no_headers(url, data, timeout, headers):
            """Mock that returns no verification headers"""
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {}  # No Content-MD5 or ETag headers
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_no_headers):
            # Should still succeed since local checksum verification passes
            upload_new_file_single(file_path, upload_info, md5_checksum)
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured

    def test_upload_404_error(self, temp_dir, get_scope_uuid):
        """Test upload failure when server returns 404 error."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        def mock_put_404(url, data, timeout, headers):
            """Mock that returns 404 error"""
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.json.return_value = {"error": "Not found"}
            mock_response.text = "404 Not Found"
            return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_404) as mock_put:
            with pytest.raises(uploadFailedException):
                upload_new_file_single(file_path, upload_info, md5_checksum)
            
            assert mock_put.call_count == MAX_TRIES
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.pending

    def test_upload_success_on_third_try(self, temp_dir, get_scope_uuid):
        """Test upload succeeding on the third attempt after two failures."""
        scope_uuid = get_scope_uuid
        
        file_path, upload_info, md5_checksum, md5_checksum_netcdf4 = create_upload_info(temp_dir, scope_uuid)
        
        call_count = 0
        
        def mock_put_fail_twice_succeed_third(url, data, timeout, headers):
            """Mock that fails twice then succeeds on third try"""
            nonlocal call_count
            call_count += 1
            
            mock_response = Mock()
            if call_count <= 2:
                # First two attempts fail with timeout
                raise urllib3.exceptions.ReadTimeoutError(None, None, "Read timeout")
            else:
                # Third attempt succeeds
                mock_response.status_code = 200
                correct_etag = f'"{md5_checksum.hexdigest()}"'
                mock_response.headers = {'ETag': correct_etag}
                return mock_response
        
        with patch('etiket_sync_agent.sync.uploader.file_uploader.client.session.put', side_effect=mock_put_fail_twice_succeed_third) as mock_put:
            upload_new_file_single(file_path, upload_info, md5_checksum)
            
            # Should have been called exactly 3 times
            assert mock_put.call_count == 3
        
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured

    def test_upload_zero_bytes_file(self, temp_dir, get_scope_uuid):
        """Test successful upload of a zero bytes (empty) file."""
        scope_uuid = get_scope_uuid
        ds_uuid = uuid.uuid4()
        file_uuid = uuid.uuid4()
        file_name = "empty_test"
        
        # Create empty file
        file_path = create_test_file(temp_dir, file_name, content="")
        version_id = 10000

        # Create dataset and file entry for empty file
        remote_dataset_create = DatasetCreateRemote(
                uuid=ds_uuid,
                name=f"Test Dataset Empty",
                description="Test dataset for empty file",
                keywords=["test"],
                attributes={},
                ranking=0,
                creator="test_user",
                scope_uuid=scope_uuid,
                collected=datetime.datetime.now()
            )
        dataset_create(remote_dataset_create)

        file_create_remote = FileCreateRemote(
            name=file_name,
            filename=os.path.basename(file_path),
            uuid=file_uuid,
            creator="test_user",
            collected=datetime.datetime.now(),
            size=0,  # Zero size
            type=FileType.JSON,
            file_generator="test",
            version_id=version_id,
            ds_uuid=ds_uuid,
            immutable=False
        )
        
        file_create(file_create_remote)
        
        upload_info = file_generate_presigned_upload_link_single(file_create_remote.uuid, version_id)
        md5_checksum = md5(file_path)
        
        # Upload the empty file
        upload_new_file_single(file_path, upload_info, md5_checksum)
        
        # Verify successful upload
        r_files = file_read(FileSelect(uuid=upload_info.uuid, version_id=upload_info.version_id))
        assert len(r_files) == 1
        assert r_files[0].status == FileStatusRem.secured
        assert r_files[0].md5_checksum == str(md5_checksum.hexdigest())
        assert r_files[0].size == 0