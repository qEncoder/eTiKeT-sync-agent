import logging, os, requests, urllib3, base64, hashlib

from typing import Optional, Any
from requests.exceptions import JSONDecodeError

from etiket_sync_agent.backends.dataset_manifest import DatasetManifest
from etiket_client.remote.endpoints.file import file_validate_upload_multi, file_validate_upload_single
from etiket_client.remote.endpoints.models.file import FileValidate, FileSignedUploadLink, FileSignedUploadLinks
from etiket_client.remote.client import client

from etiket_client.exceptions import uploadFailedException
from etiket_client.sync.base.checksums.any import md5

logger = logging.getLogger(__name__)

MAX_TRIES = 3

def upload_new_file_single(file_raw_name, upload_info: FileSignedUploadLink, md5_checksum : Any, md5_checksum_netcdf4 : Optional[Any] = None):
    # Calculate timeout based on file size with a minimum and maximum limit
    timeout = max(10, min(os.stat(file_raw_name).st_size / 100_000, 1800))

    
    with open(file_raw_name, 'rb') as file:
        for n_tries in range(MAX_TRIES):
            base64_md5_checksum = base64.b64encode(md5_checksum.digest()).decode('utf-8')
            try:
                header = {
                    'x-ms-blob-type': 'BlockBlob',
                    # 'Content-MD5': base64_md5_checksum, # this does currently not work for AWS -- server code update needed!
                    'Content-Type': 'application/octet-stream',
                    'Content-Length': str(os.stat(file_raw_name).st_size)
                }
                # in case of minio the header should be empty
                if upload_info.url.startswith('https://minio') or upload_info.url.startswith('http://minio'): # unit tests ...
                    header = {}
                response = client.session.put(upload_info.url, data=file, timeout=timeout, headers=header)

                if response.status_code == 200 or response.status_code == 201:
                    logger.info(f"Checksum verification started for HDF5/NetCDF file: {file_raw_name}")
                    # recalculate checksum to ensure the contents were not changed during upload
                    md5_checksum_recalculated = md5(file_raw_name)
                    md5_checksum_recalculated_base64 = base64.b64encode(md5_checksum_recalculated.digest()).decode('utf-8')
                    success = True # Assume success, set to False on mismatch
                    if 'Content-MD5' in response.headers:
                        server_md5 = response.headers['Content-MD5']
                        logger.debug(f"Verifying against server Content-MD5 for {file_raw_name}. Server: {server_md5}, Client recalculated: {md5_checksum_recalculated_base64}")
                        if server_md5 != md5_checksum_recalculated_base64:
                            logger.warning(f"Content-MD5 mismatch for {file_raw_name}. Server: {server_md5}, Client recalculated: {md5_checksum_recalculated_base64}")
                            success = False
                        else:
                            logger.debug(f"Content-MD5 match for {file_raw_name}.")
                    elif 'ETag' in response.headers:
                        server_md5_etag = response.headers['ETag'].strip('"') # Remove surrounding quotes
                        local_md5_hex = md5_checksum_recalculated.hexdigest()
                        print("SERVER MD5 ETAG", server_md5_etag)
                        print("LOCAL  MD5 HEX", local_md5_hex)
                        logger.debug(f"Verifying against server ETag for {file_raw_name}. Server ETag (processed): {server_md5_etag}, Client recalculated hex: {local_md5_hex}")
                        if server_md5_etag.lower() != local_md5_hex.lower(): # Case-insensitive comparison
                            logger.warning(f"ETag mismatch for {file_raw_name}. Server ETag (processed): {server_md5_etag}, Client recalculated hex: {local_md5_hex}")
                            success = False
                        else:
                            logger.debug(f"ETag match for {file_raw_name}.")

                    logger.debug(f"Comparing for {file_raw_name}. Recalculated Base64: {md5_checksum_recalculated_base64}, Pre-upload Base64: {base64_md5_checksum}")
                    if md5_checksum_recalculated_base64 != base64_md5_checksum:
                        logger.warning(f"Local checksum mismatch for {file_raw_name} (file changed during upload). Recalculated Base64: {md5_checksum_recalculated_base64}, Pre-upload Base64: {base64_md5_checksum}")
                        success = False
                    else:
                        logger.debug(f"Local checksums match for {file_raw_name} (file unchanged during upload operation).")
                    
                    if success:
                        logger.info(f"Checksum verification successful for {file_raw_name}.")
                    else:
                        logger.error(f"Checksum verification FAILED for {file_raw_name}.")
                else:
                    success = False
                    try:
                        logger.warning('Failed to upload a file with name (%s).\nRAW JSON response :: %s', file_raw_name, response.json())
                    except JSONDecodeError:
                        logger.warning('Failed to upload a file with name (%s).\nRAW response :: %s', file_raw_name, response.text)
            except (TimeoutError, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout):
                logger.warning('Timeout while uploading a file with name (%s).\n File size :: %s bytes, timeout :: %s', file_raw_name, os.stat(file_raw_name).st_size, timeout)
                success = False
            except Exception:
                logger.exception('Unexpected error while uploading a file with name (%s)', file_raw_name)
                success = False

            if success:
                break
            elif n_tries == MAX_TRIES - 1:
                raise uploadFailedException('Failed to upload file after 3 attempts.')

    if md5_checksum_netcdf4:
        file_validate_upload_single(FileValidate(uuid=upload_info.uuid, version_id=upload_info.version_id, upload_id='', md5_checksum=md5_checksum_netcdf4.hexdigest(), etags=[]))
    else:
        file_validate_upload_single(FileValidate(uuid=upload_info.uuid, version_id=upload_info.version_id, upload_id='', md5_checksum=md5_checksum.hexdigest(), etags=[]))

# # TODO on the server side, make sure only one client can upload.
# def upload_new_file_multi(file_raw_name, upload_info  : FileSignedUploadLinks, md5_checksum, ntries = 0):
#     try:
#         n_parts = len(upload_info.presigned_urls)
#         chunk_size = upload_info.part_size
#         etags = []
#         with open(file_raw_name, 'rb') as file:
#             for i in range(n_parts):
#                 file.seek(i * chunk_size)
#                 data = file.read(chunk_size)

#                 for n_tries in range(3):
#                     success, response = upload_chunk(upload_info.presigned_urls[i], data)
#                     if n_tries == 2 and success is False:
#                         raise uploadFailedException('Failed to upload file.')
#                     if success is True:
#                         break

#                 etags.append(str(response.headers['ETag']))
        
#         fv = FileValidate(uuid=upload_info.uuid, version_id=upload_info.version_id,
#                             upload_id=upload_info.upload_id, md5_checksum=md5_checksum,
#                             etags=etags)
#         file_validate_upload_multi(fv)

#     except Exception as e:
#         if ntries < 3:
#             logger.warning('Failed to upload file with name %s.\n Error message :: %s, try %s (and trying again).\n', file_raw_name, e, ntries)
#             upload_new_file_multi(file_raw_name, upload_info, ntries+1)
#         else :
#             logger.exception('Failed to upload file with name %s.\n', file_raw_name)
#             raise e
    
# def upload_chunk(url, data: bytes):
#     header = {
#         'x-ms-blob-type': 'BlockBlob',
#         'Content-Type': 'application/octet-stream', #only necessary if you are using a stream 
#         'Content-Length': str(len(data))
#     }
#     response = client.session.put(url, data=data, timeout=400, headers=header) # assume that the upload speed is > 100KB/s

#     if response.status_code >=400:
#         response_json = None
#         if response:
#             response_json = response.json()
#         logging.warning('Failed to upload a chunk to url with hash (%s).\nRAW JSON response :: %s', hash(url), response_json)
#         return False, response
#     return True, response