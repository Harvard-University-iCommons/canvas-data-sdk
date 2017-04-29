from .hmac_auth import CanvasDataHMACAuth, API_ROOT
import requests
from requests.exceptions import RequestException, ConnectionError
from .exceptions import CanvasDataAPIError, MissingCredentialsError, APIConnectionError
import os
import gzip


class CanvasDataAPI(object):

    def __init__(self, api_key, api_secret):
        if not api_key or not api_secret:
            raise MissingCredentialsError(self)

        self.api_key = api_key
        self.api_secret = api_secret

    def get_schema_versions(self):
        """Get the list of all available schema versions."""
        url = '{}/api/schema'.format(API_ROOT)
        try:
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                schema_versions = response.json()
                return schema_versions
            else:
                response_data = response.json()
                raise CanvasDataAPIError(response_data['message'])
        except ConnectionError as e:
            raise APIConnectionError("A connection error occurred", e)
        except RequestException as e:
            raise CanvasDataAPIError("A generic requests error occurred", e)

    def get_schema(self, version, key_on_tablenames=False):
        """
        Get a particular version of the schema.
        Note that the keys in the returned data structure are usually, but not always,
        the same as the table names. If you'd rather have the keys in the returned data
        structure always exactly match the table names, set `key_on_tablenames=True`
        """
        url = '{}/api/schema/{}'.format(API_ROOT, version)
        try:
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                schema = response.json()
                if key_on_tablenames:
                    fixed_schema = {}
                    for k, v in schema['schema'].iteritems():
                        fixed_schema[v['tableName']] = v
                    return fixed_schema

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
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
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
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
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

    def get_sync_file_urls(self, account_id='self'):
        """Get a list of file URLs that constitute a complete snapshot of the current data"""
        url = '{}/api/account/{}/file/sync'.format(API_ROOT, account_id)
        try:
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
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

    def download_files(self, account_id='self', dump_id=None, table_name=None, directory='./downloads', include_requests=True):
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
                            if os.path.isfile(target_file):
                                # the file already exists in the download directory
                                continue
                            else:
                                r = requests.get(file['url'], stream=True)
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
                    if os.path.isfile(target_file):
                        # the file already exists in the download directory
                        continue
                    else:
                        r = requests.get(file['url'], stream=True)
                        with open(target_file, 'wb') as fd:
                            for chunk in r.iter_content(chunk_size=128):
                                fd.write(chunk)

        else:
            raise CanvasDataAPIError("Neither dump_id or table_name was specified; must specify at least one.")

        return local_files

    def get_data_for_table(self, table_name, account_id='self', dump_id='latest', data_directory='./data'):
        """Decompresses and concatenates the dump files for a particular table and writes the resulting data to a text file."""

        # get the raw data files
        files = self.download_files(account_id=account_id, table_name=table_name, dump_id=dump_id)

        outfilename = os.path.join(data_directory, '{}.txt'.format(table_name))

        with open(outfilename, 'w') as outfile:
            # gunzip each file and write the data to the output file
            for infilename in files:
                with gzip.open(infilename, 'rb') as infile:
                    outfile.write(infile.read())

        return outfilename
