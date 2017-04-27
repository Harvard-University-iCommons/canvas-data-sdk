from .hmac_auth import CanvasDataHMACAuth, API_ROOT
import requests
from requests.exceptions import RequestException, ConnectionError
from .exceptions import CanvasDataAPIError, MissingCredentialsError, APIConnectionError

# Some tables have one name in the schema (keys) and a different name in the dumps (values)
TABLE_MAP = {
    'course': 'course_dim',
    'user': 'user_dim',
    'enrollment_term': 'enrollment_term_dim',
    'course_section': 'course_section_dim',
    'account': 'account_dim',
    'conversation_message': 'conversation_message_dim',
    'enrollment_rollup': 'enrollment_rollup_dim',
    'conversation': 'conversation_dim',
    'conversation_message_participant': 'conversation_message_participant_fact',
}


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

    def get_schema(self, version):
        """Get a particular version of the schema."""
        url = '{}/api/schema/{}'.format(API_ROOT, version)
        try:
            response = requests.get(url, auth=CanvasDataHMACAuth(self.api_key, self.api_secret))
            if response.status_code == 200:
                schema = response.json()
                return schema
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

    def fix_table_name(self, table_name):
        """Returns the correct dump table name given a schema table name."""
        return TABLE_MAP.get(table_name, table_name)
