
import exceptions, model, util

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
    
    def get_base_image(self, id):
        """
        Get a specific base image by ID.
        
        Args:
            id (string): The base image's ID.
        
        Returns:
            An instance of as_client.BaseImage representing the given base image.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._fetch_resource(model.BaseImage, id)
    
    def get_base_images(self, skip=None, limit=None, page_size=None):
        """
        Get a list of existing base images.
        
        This method supports either retrieving a specific subset of the existing
        base images (using the "skip" and "limit" parameters), or retrieving the
        entire set of existing base images.
        
        If retrieving the entire set of base images, the request is
        automatically and transparently paginated. The page size when doing so
        can be controlled using the "page_size" parameter.
        
        Args:
            skip (integer, optional): The number of base images to skip at the
                start of the list.
            
            limit (integer, optional): The maximum number of base images to
                return.
            
            page_size (integer, optional): Automatically paginate the request
                with pages of this size.
        
        Returns:
            A sequence of as_model.BaseImage instances.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._get_resources(model.BaseImage, skip, limit, page_size)
    
    def get_model(self, id):
        """
        Get a specific model by ID.
        
        Args:
            id (string): The model's ID.
        
        Returns:
            An instance of as_client.Model representing the given model.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._fetch_resource(model.Model, id)
    
    def get_models(self, skip=None, limit=None, page_size=None):
        """
        Get a list of existing models.
        
        This method supports either retrieving a specific subset of the existing
        models (using the "skip" and "limit" parameters), or retrieving the
        entire set of existing models.
        
        If retrieving the entire set of models, the request is automatically
        and transparently paginated. The page size when doing so can be
        controlled using the "page_size" parameter.
        
        Args:
            skip (integer, optional): The number of models to skip at the
                start of the list.
            
            limit (integer, optional): The maximum number of models to
                return.
            
            page_size (integer, optional): Automatically paginate the request
                with pages of this size.
        
        Returns:
            A sequence of as_model.Model instances.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._get_resources(model.Model, skip, limit, page_size)
    
    def install_model(self, path, manifest=None, include_hidden=False):
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
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        if os.path.isdir(path):
            logger.debug('Generating new model tar/gzip file from files at path %s', path)
            with tempfile.TemporaryFile() as f:
                with tarfile.open(fileobj=f, mode='w:gz') as tar_file:
                    for root, dirs, files in os.walk(path):
                        for file_ in files:
                            source_path = os.path.join(root, file_)
                            if not include_hidden and util.path_is_hidden(source_path): # ignore hidden files
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
    
    def get_workflow(self, id):
        """
        Get a specific workflow by ID.
        
        Args:
            id (string): The workflow's ID.
        
        Returns:
            An instance of as_client.Workflow representing the given workflow.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._fetch_resource(model.Workflow, id)
    
    def get_workflows(self, skip=None, limit=None, page_size=None):
        """
        Get a list of existing workflows.
        
        This method supports either retrieving a specific subset of the existing
        workflows (using the "skip" and "limit" parameters), or retrieving the
        entire set of existing workflows.
        
        If retrieving the entire set of workflows, the request is automatically
        and transparently paginated. The page size when doing so can be
        controlled using the "page_size" parameter.
        
        Args:
            skip (integer, optional): The number of workflows to skip at the
                start of the list.
            
            limit (integer, optional): The maximum number of workflows to
                return.
            
            page_size (integer, optional): Automatically paginate the request
                with pages of this size.
        
        Returns:
            A sequence of as_model.Workflow instances.
        
        Raises:
            exceptions.RequestError: if an HTTP "client error" (4XX) status code
                is returned by the server.
            exceptions.ServerError: if an HTTP "server error" (5XX) status code
                is returned by the server.
        """
        return self._get_resources(model.Workflow, skip, limit, page_size)
    
    def post_workflow(self, workflow):
        pass # TODO
    
    def run_workflow(self, id):
        pass # TODO
    
    def _fetch_resource(self, type_, id_, instance=None):
        assert hasattr(type_, '_url_path')
        assert callable(getattr(type_, '_update', None))
        
        url = util.append_path_to_url(self._base_url, type_._url_path, id_)
        json = self._check_response(self._session.get(url=url))
        
        if instance is None:
            instance = type_()
        
        return instance._update(self, json)
    
    def _get_resources(self, type_, skip, limit, page_size):
        assert hasattr(type_, '_url_path')
        assert hasattr(type_, '_collection')
        assert callable(getattr(type_, '_update', None))
        
        if skip is None and limit is None:
            return model._ResourceCollection(self, type_, page_size)
        elif page_size is not None:
            raise ValueError('The "page_size" parameter cannot be used if the "skip" or "limit" parameters are used.')
        else:
            query = { 'skip': skip, 'limit': limit }
            
            url = util.append_path_to_url(self._base_url, type_._url_path)
            json = self._check_response(self._session.get(url=url, params=query))
            
            return model._ResourceList(self, type_, json)
    
    """def _post_resource(self, ...):
        pass"""
    
    def _post_model_archive(self, archive_file, name, mime_type):
        url = util.append_path_to_url(self._base_url, 'models')
        logger.debug('Uploading new model to %s...', url)
        files = { 'archive': (name, archive_file, mime_type, {}) }
        
        response = self._session.post(url=url, files=files)
        logger.log(TRACE, 'Response: %s', response.text)
        
        print self._check_response(response) # TODO: handle valid response
    
    def _check_response(self, response):
        if 400 <= response.status_code < 500:
            raise exceptions.RequestError(**response.json())
        elif 500 <= response.status_code:
            raise exceptions.ServerError(**response.json())
        
        return response.json()
    
    @property
    def base_url(self):
        return self._base_url
    
    @property
    def session(self):
        return self._session
