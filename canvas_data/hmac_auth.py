import requests
import sys
import base64
import hmac
import hashlib
from datetime import datetime

try:
    from urllib import urlencode
except:
    # For Python 3
    from urllib.parse import urlencode

try:
    from urlparse import parse_qs
except:
    # For Python 3
    from urllib.parse import parse_qs

API_ROOT = 'https://api.inshosteddata.com'
SIGNATURE_MESSAGE_TEMPLATE = '''GET
api.inshosteddata.com


{resource_path}
{args}
{date}
{api_secret}'''


class CanvasDataHMACAuth(requests.auth.AuthBase):
    """Attaches HMAC Authentication to the given Request object."""

    def __init__(self, api_key, api_secret, api_root=API_ROOT):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_root = api_root
        if sys.version_info >= (3, 0):
            self.api_key = bytes(api_key, 'utf-8')
            self.api_secret = bytes(api_secret, 'utf-8')

        self.req_date = datetime.utcnow().strftime('%a, %d %b %y %H:%M:%S GMT')

    def __call__(self, r):
        # build the auth header
        path = r.url
        if path.startswith(self.api_root):
            path = path[len(self.api_root):]

        # if the URL has a query string, remove it and sort the key-value pairs
        args_str = ''
        if '?' in path:
            [path, qs] = path.split('?')
            kv_pairs = qs.split('&')
            kv_pairs.sort()
            args_str = '&'.join(kv_pairs)

        sig_body = SIGNATURE_MESSAGE_TEMPLATE.format(
            resource_path=path,
            api_secret=self.api_secret,
            args=args_str,
            date=self.req_date,
        )
        signature = base64.b64encode(
            hmac.new(
                self.api_secret,
                msg=sig_body,
                digestmod=hashlib.sha256).digest())
        if sys.version_info >= (3, 0):
            signature = signature.decode('utf-8')

        r.headers['Authorization'] = 'HMACAuth {0}:{1}'.format(self.api_key, signature)
        r.headers['Date'] = self.req_date

        return r
