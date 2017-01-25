
class Error(Exception):
    """
    Base class of all client exception classes.
    
    Attributes:
        response: The underlying requests response that triggered the exception.
        status_code: The API-supplied status code.
        kwargs: Any additional properties supplied in the API's response.
    """
    def __init__(self, response, message=None, statuscode=None, **kwargs):
        super(Error, self).__init__(message)
        
        self.response = response
        self.status_code = statuscode
        self.kwargs = kwargs

class RequestError(Error):
    """
    Exception class raised when an error occurs due to a problem with the
    client's request.
    """
    pass

class ServerError(Error):
    """
    Exception class raised when an error occurs within the API server.
    """
    pass
