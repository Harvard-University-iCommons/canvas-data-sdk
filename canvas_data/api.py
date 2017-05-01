import gzip
import logging
import os
import time

import requests
from requests.exceptions import ConnectionError, RequestException

from .exceptions import (APIConnectionError, CanvasDataAPIError,
                         MissingCredentialsError)
from .hmac_auth import API_ROOT, CanvasDataHMACAuth

logger = logging.getLogger(__name__)


def retry(func):
    """File download retry decorator"""
    def retried_func(*args, **kwargs):
        MAX_TRIES = 3
        tries = 0
        while True:
            try:
                resp = func(*args, **kwargs)
                if resp.status_code != 200 and tries < MAX_TRIES:
                    logger.warning("Got a non-200 response ({}) - going to retry.".format(resp.status_code))
                    tries += 1
                    time.sleep(3)
                    continue

            except ConnectionError as e:
                resp = None
                if tries < MAX_TRIES:
                    tries += 1
                    logger.exception("ConnectionError - %d/%d tries", tries, MAX_TRIES)
                    time.sleep(3)
                    continue
                else:
                    logger.exception("ConnectionError - reached the retry limit")
                    raise e
            break

        return resp
    return retried_func


@retry
def _get_with_retries(*args, **kwargs):
    return requests.get(*args, **kwargs)


class CanvasDataAPI(object):

    def __init__(self, api_key, api_secret):
        if not api_key or not api_secret:
            raise MissingCredentialsError(self)

        self.api_key = api_key
        self.api_secret = api_secret

        self.schema = {}
        self.schema_versions = None

    def get_schema_versions(self):
        """Get the list of all available schema versions."""
        url = '{}/api/schema'.format(API_ROOT)
        if self.schema_versions:
            return self.schema_versions
        else:
            try:
                response = _get_with_retries(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
                if response.status_code == 200:
                    schema_versions = response.json()
                    self.schema_versions = schema_versions
                    return schema_versions
                else:
                    response_data = response.json()
                    raise CanvasDataAPIError(response_data['message'])
            except ConnectionError as e:
                raise APIConnectionError("A connection error occurred", e)
            except RequestException as e:
                raise CanvasDataAPIError("A generic requests error occurred", e)

    def get_schema(self, version='latest', key_on_tablenames=False):
        """
        Get a particular version of the schema.
        Note that the keys in the returned data structure are usually, but not always,
        the same as the table names. If you'd rather have the keys in the returned data
        structure always exactly match the table names, set `key_on_tablenames=True`
        """
        url = '{}/api/schema/{}'.format(API_ROOT, version)
        cache_key = '{}/{}'.format(version, key_on_tablenames)
        if cache_key in self.schema:
            return self.schema[cache_key]
        else:
            try:
                response = _get_with_retries(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
                if response.status_code == 200:
                    schema = response.json()
                    if key_on_tablenames:
                        fixed_schema = {}
                        for k, v in schema['schema'].iteritems():
                            fixed_schema[v['tableName']] = v
                        self.schema[cache_key] = fixed_schema
                        return fixed_schema

                    self.schema[cache_key] = schema['schema']
                    return schema['schema']

                else:
                    response_data = response.json()
                    raise CanvasDataAPIError(response_data['message'])
            except ConnectionError as e:
                raise APIConnectionError("A connection error occurred", e)
            except RequestException as e:
                raise CanvasDataAPIError("A generic requests error occurred", e)

    def get_dumps(self, account_id='self'):
        """Get a list of all dumps"""
        url = '{}/api/account/{}/dump'.format(API_ROOT, account_id)
        try:
            response = _get_with_retries(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                dumps = response.json()
                return dumps
            else:
                response_data = response.json()
                raise CanvasDataAPIError(response_data['message'])
        except ConnectionError as e:
            raise APIConnectionError("A connection error occurred", e)
        except RequestException as e:
            raise CanvasDataAPIError("A generic requests error occurred", e)

    def get_file_urls(self, account_id='self', **kwargs):
        """Get a list of file URLs, either by dump_id (or latest) or by table_name."""
        if kwargs.get('dump_id'):
            if kwargs['dump_id'] == 'latest':
                url = '{}/api/account/{}/file/latest'.format(API_ROOT, account_id)
            else:
                url = '{}/api/account/{}/file/byDump/{}'.format(API_ROOT, account_id, kwargs['dump_id'])
        elif kwargs.get('table_name'):
            url = '{}/api/account/{}/file/byTable/{}'.format(API_ROOT, account_id, kwargs['table_name'])
        else:
            raise CanvasDataAPIError("Must pass either dump_id or table_name")
        try:
            response = _get_with_retries(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                files = response.json()
                return files
            else:
                response_data = response.json()
                raise CanvasDataAPIError(response_data['message'])
        except ConnectionError as e:
            raise APIConnectionError("A connection error occurred")
        except RequestException as e:
            raise CanvasDataAPIError("A generic requests error occurred", e)

    def get_sync_file_urls(self, account_id='self'):
        """Get a list of file URLs that constitute a complete snapshot of the current data"""
        url = '{}/api/account/{}/file/sync'.format(API_ROOT, account_id)
        try:
            response = _get_with_retries(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                files = response.json()
                return files
            else:
                response_data = response.json()
                raise CanvasDataAPIError(response_data['message'])
        except ConnectionError as e:
            raise APIConnectionError("A connection error occurred", e)
        except RequestException as e:
            raise CanvasDataAPIError("A generic requests error occurred", e)

    def download_files(self, account_id='self', dump_id=None, table_name=None, directory='./downloads', include_requests=True, force=False):
        """Download all of the files for a specific dump, all of the files for a specific table, or the files for a specific table from a specific dump."""
        local_files = []
        if dump_id:
            dump_files = self.get_file_urls(account_id=account_id, dump_id=dump_id)
            for dump_table_name, artifacts in dump_files['artifactsByTable'].iteritems():
                if table_name and table_name != dump_table_name:
                    continue
                else:
                    if dump_table_name == 'requests' and not include_requests:
                        continue
                    else:
                        # download the files
                        for file in artifacts['files']:
                            target_file = '{}/{}'.format(directory, file['filename'])
                            local_files.append(target_file)
                            if os.path.isfile(target_file) and not force:
                                logger.debug("Not downloading %s because it already exists.", target_file)
                                continue
                            else:
                                r = _get_with_retries(file['url'], stream=True)
                                # TODO: check to make sure download was successful
                                with open(target_file, 'wb') as fd:
                                    for chunk in r.iter_content(chunk_size=128):
                                        fd.write(chunk)

        elif table_name:
            # no dump ID was specified; just get all of the files for the specified table
            dump_files = self.get_file_urls(account_id=account_id, table_name=table_name)
            for dump in dump_files['history']:
                for file in dump['files']:
                    target_file = '{}/{}'.format(directory, file['filename'])
                    local_files.append(target_file)
                    if os.path.isfile(target_file) and not force:
                        logger.debug("Not downloading %s because it already exists.", target_file)
                        continue
                    else:
                        r = _get_with_retries(file['url'], stream=True)
                        with open(target_file, 'wb') as fd:
                            for chunk in r.iter_content(chunk_size=128):
                                fd.write(chunk)

        else:
            raise CanvasDataAPIError("Neither dump_id or table_name was specified; must specify at least one.")

        return local_files

    def get_data_for_table(self, table_name, account_id='self', dump_id='latest', data_directory='./data', force=False):
        """Decompresses and concatenates the dump files for a particular table and writes the resulting data to a text file."""
        outfilename = os.path.join(data_directory, '{}.txt'.format(table_name))

        if os.path.isfile(outfilename) and not force:
            logger.debug("Not overwriting %s because it already exists.", outfilename)
            return outfilename
        else:
            # get the raw data files
            files = self.download_files(account_id=account_id, table_name=table_name, dump_id=dump_id)

            with open(outfilename, 'w') as outfile:
                # gunzip each file and write the data to the output file
                for infilename in files:
                    with gzip.open(infilename, 'rb') as infile:
                        outfile.write(infile.read())

            return outfilename

    def get_data_for_dump(self, dump_id='latest', account_id='self', data_directory='./data', include_requests=False, force=False):
        """Decompresses and concatenates the dump files for all of the tables in a particular dump."""
        dump = self.get_file_urls(dump_id=dump_id, account_id=account_id)
        dump_table_names = dump['artifactsByTable'].keys()

        outfiles = []
        for table_name in dump_table_names:
            if table_name == 'requests' and not include_requests:
                continue
            filename = self.get_data_for_table(table_name=table_name, account_id=account_id, dump_id=dump_id, data_directory=data_directory)
            outfiles.append(filename)

        return outfiles
