
class Error(Exception):
    def __init__(self, response, message=None, statuscode=None, **kwargs):
        super(Error, self).__init__(message)
        
        self.response = response
        self.status_code = statuscode
        self.kwargs = kwargs

class RequestError(Error):
    pass

class ServerError(Error):
    pass
