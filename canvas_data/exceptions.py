
class CanvasDataAPIError(Exception):
    '''Basic Canvas Data API errors'''

    def __init__(self, msg=None):
        if msg is None:
            msg = "An error occurred when calling the Canvas Data API"
        super(CanvasDataAPIError, self).__init__(msg)


class MissingCredentialsError(CanvasDataAPIError):
    '''Raised when either the api_key or api_secret is missing'''

    def __init__(self, msg=None):
        if msg is None:
            msg = "The api_key or api_secret is missing"
        super(CanvasDataAPIError, self).__init__(msg)


class APIConnectionError(CanvasDataAPIError):
    '''Raised when the underlying HTTP request fails'''
    def __init__(self, msg=None):
        if msg is None:
            msg = "There was an API connection error"
        super(CanvasDataAPIError, self).__init__(msg)
