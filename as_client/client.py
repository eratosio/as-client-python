
import util

import requests

import json, logging, os, posixpath, tarfile, tempfile, time, zipfile
from cStringIO import StringIO

logger = logging.getLogger(__name__)
TRACE = 5

def _tarinfo_filter(tarinfo):
    # Strip user info from files.
    tarinfo.uid = tarinfo.gid = 0
    tarinfo.uname = tarinfo.gname = 'root'
    return tarinfo

class Error(Exception):
    def __init__(self, message=None, statuscode=None, **kwargs):
        super(Error, self).__init__(message)
        
        self.status_code = statuscode
        self.kwargs = kwargs

class RequestError(Error):
    pass

class ServerError(Error):
    pass

class Client(object):
    """
    A client for the Analysis Services API.
    
    Attributes:
        base_url (string, read only): The base URL of the API the client is
            configured for.
        
        session (read_only): The underlying Requests session.
    """
    
    def __init__(self, base_url, auth=None):
        """
        Initialise the client.
        
        Args:
            base_url (string): The API's base URL
            
            auth (optional): the Python requests authoriser to use to authorise
                the API requests.
        """
        self._base_url = base_url
        
        self._session = requests.Session()
        self._session.auth = auth
    
    def install_model(self, path, manifest=None):
        """
        Install a new model.
        
        Args:
            path (string): The path to the model files to install.
                
                The path may point either to a directory containing the files,
                to a ZIP file containing the files, or to a tar/gzip file
                containing the files.

            manifest (dict, optional): The model's manifest.
            
                If omitted, the given directory, ZIP file or tar/gzip file MUST
                contain a manifest.json file containing the model's manifest.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        if os.path.isdir(path):
            logger.debug('Generating new model zip file from files at path %s', path)
            with tempfile.TemporaryFile() as f:
                with tarfile.open(fileobj=f, mode='w:gz') as tar_file:
                    for root, dirs, files in os.walk(path):
                        for file_ in files:
                            source_path = os.path.join(root, file_)
                            if util.path_is_hidden(source_path): # ignore hidden files
                               continue
                            
                            dest_path = os.path.relpath(source_path, path)
                            if manifest is None or dest_path != 'manifest.json':
                                tar_file.add(source_path, arcname=dest_path, recursive=False, filter=_tarinfo_filter)
                    
                    if manifest is not None:
                        manifest = json.dumps(manifest)
                        tarinfo = tarfile.TarInfo('manifest.json')
                        tarinfo.size = len(manifest)
                        tarinfo.uid = tarinfo.gid = 0
                        tarinfo.uname = tarinfo.gname = 'root'
                        tarinfo.mtime = time.time()
                        tarinfo.mode = 0664
                        tarinfo.type = tarfile.REGTYPE
                        tar_file.addfile(tarinfo, StringIO(manifest))
                
                f.seek(0)
                self._post_model_archive(f, 'model.tar.gz', 'application/gzip')
        elif zipfile.is_zipfile(path):
            logger.debug('Uploading model zip file %s', path)
            with open(path, 'rb') as f:
                self._post_model_archive(f, 'model.zip', 'application/zip')
        elif tarfile.is_tarfile(path):
            logger.debug('Uploading model tar/gzip file %s', path)
            with open(path, 'rb') as f:
                self._post_model_archive(f, 'model.tar.gz', 'application/gzip')
        else:
            raise ValueError('Path {} does not refer to a directory, zip file or tar/gzip file.'.format(path))
    
    
    def _post_model_archive(self, archive_file, name, mime_type):
        url = posixpath.join(self._base_url, 'models')
        logger.debug('Uploading new model to %s...', url)
        files = { 'archive': (name, archive_file, mime_type, {}) }
        
        response = self._session.post(url=url, files=files)
        logger.log(TRACE, 'Response: %s', response.text)
        
        if 400 <= response.status_code < 500:
            raise RequestError(**response.json())
        elif 500 <= response.status_code:
            raise ServerError(**response.json())
        
        print response # TODO: handle valid response
    
    @property
    def base_url(self):
        return self._base_url
    
    @property
    def session(self):
        return self._session
