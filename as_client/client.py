
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
        base_url: The "base URL" of the API instance the client is connected to.
        session: The underlying Python requests' "session" object.
    """
    
    def __init__(self, base_url, auth=None):
        """
        Initialise the client.
        
        Args:
            base_url:   The API's base URL
            auth:       The Python requests authoriser to use to authorise the API requests.
        """
        self._base_url = base_url
        
        self._session = requests.Session()
        self._session.auth = auth
    
    def get_base_image(self, id):
        """
        Get a specific base image by ID.
        
        Args:
            id: The base image's ID.
        
        Returns:
            An instance of as_client.BaseImage representing the given base image.
        
        Raises:
            as_client.RequestError: If an HTTP "client error" (4XX) status code is returned by the server.
            as_client.ServerError: If an HTTP "server error" (5XX) status code is returned by the server.
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
            skip: The number of base images to skip at the start of the list.
            limit: The maximum number of base images to return.
            page_size: Automatically paginate the request, and ensure individual
                requests return pages at most this size.
        
        Returns:
            A sequence of as_model.BaseImage instances.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        return self._get_resources(model.BaseImage, skip, limit, page_size)
    
    def get_model(self, id):
        """
        Get a specific model by ID.
        
        Args:
            id: The model's ID.
        
        Returns:
            An instance of as_client.Model representing the given model.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
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
            skip: The number of base images to skip at the start of the list.
            limit: The maximum number of base images to return.
            page_size: Automatically paginate the request, and ensure individual
                requests return pages at most this size.
        
        Returns:
            A sequence of as_model.Model instances.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        return self._get_resources(model.Model, skip, limit, page_size)
    
    def install_model(self, path, manifest=None, include_hidden=False):
        """
        Install a new model.
        
        Args:
            path: The path to the model files to install. The path may point
                either to a directory containing the files, to a ZIP file
                containing the files, or to a tar/gzip file containing the files.
            manifest: The model's manifest. If omitted, the given directory, ZIP
                file or tar/gzip file MUST contain a manifest.json file
                containing the model's manifest.
            include_hidden: If True and "path" points to a directory, then
                hidden files within that directory are included as part of the
                model. Otherwise, they are ignored.
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
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
                return self._post_model_archive(f, 'model.tar.gz', 'application/gzip')
        elif zipfile.is_zipfile(path):
            logger.debug('Uploading model zip file %s', path)
            with open(path, 'rb') as f:
                return self._post_model_archive(f, 'model.zip', 'application/zip')
        elif tarfile.is_tarfile(path):
            logger.debug('Uploading model tar/gzip file %s', path)
            with open(path, 'rb') as f:
                return self._post_model_archive(f, 'model.tar.gz', 'application/gzip')
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
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
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
            skip: The number of base images to skip at the start of the list.
            limit: The maximum number of base images to return.
            page_size: Automatically paginate the request, and ensure individual
                requests return pages at most this size.
        
        Returns:
            A sequence of as_model.Workflow instances.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        return self._get_resources(model.Workflow, skip, limit, page_size)
    
    def post_workflow(self, workflow):
        """
        Post a new workflow to the analysis service.
        
        Args:
            workflow: An instance of the Workflow class to be posted to the
                analysis service.
        
        Returns:
            The same Worfklow instance, updated with any new properties
            generated by the analysis service.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        return self._post_resource(workflow)
    
    def run_workflow(self, workflow, debug=False):
        """
        Requests synchronous execution of a workflow.
        
        The workflow to be executed may be specified either with a string ID, or
        as an instance of the Workflow class. If a Workflow instance is given,
        an attempt is made to first find a matching workflow to execute. If no
        existing workflow is found, a new one is generated then executed.
        
        Args:
            workflow: The ID of the workflow to execute, or a Workflow instance
                describing the workflow to execute.
            debug: If true, the workflow is run in "debug" mode (which causes
                additional log messages and output data to be returned in the
                response).
        
        Returns:
            An instance of WorkflowResult representing the results of executing
            the workflow.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        debug = { True: 'true', False: 'false' }.get(debug, None)
        params = { 'debug': debug }
        
        # If passed a Workflow instance...
        if isinstance(workflow, model.Workflow):
            # ... attempt to run it if it has a known ID ...
            if workflow.id is not None:
                url = util.append_path_to_url(self._base_url, 'workflows', workflow.id, 'results')
                response = self._session.get(url=url, params=params)
            
            # ...otherwise, if ID is unknown or workflow doesn't exist, create it.
            if workflow.id is None or response.status_code == 404:
                workflow = self.post_workflow(workflow).id
        
        # If by this point "workflow" is an ID string (not a Workflow instance),
        # either by being passed in a such or as retrieved when creating the
        # workflow, run the workflow with the given ID.
        if isinstance(workflow, (str, basestring)):
            url = util.append_path_to_url(self._base_url, 'workflows', workflow, 'results')
            response = self._session.get(url=url, params=params)
        
        return model.WorkflowResults(self, self._check_response(response))
    
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
    
    def _post_resource(self, resource):
        assert hasattr(resource.__class__, '_url_path')
        assert callable(getattr(resource, '_serialise', None))
        
        json = resource._serialise(include_id=False)
        url = util.append_path_to_url(self._base_url, resource.__class__._url_path)
        
        json = self._check_response(self._session.post(url=url, json=json))
        
        return resource._update(self, json)
    
    def _post_model_archive(self, archive_file, name, mime_type):
        url = util.append_path_to_url(self._base_url, 'models')
        logger.debug('Uploading new model to %s...', url)
        files = { 'archive': (name, archive_file, mime_type, {}) }
        
        response = self._session.post(url=url, files=files)
        logger.log(TRACE, 'Response: %s', response.text)
        
        return model.ModelInstallationResult(self, self._check_response(response))
    
    def _check_response(self, response):
        if 400 <= response.status_code < 500:
            raise exceptions.RequestError(response, **response.json())
        elif 500 <= response.status_code:
            raise exceptions.ServerError(response, **response.json())
        
        return response.json()
    
    base_url = property(lambda self: self._base_url)
    session = property(lambda self: self._session)
