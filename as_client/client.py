
import exceptions, model, util

import requests

import collections, json, logging, os, posixpath, tarfile, tempfile, time, zipfile
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
    
    def __init__(self, base_url, session=None, auth=None):
        """
        Initialise the client.
        
        Args:
            base_url:   The API's base URL
            session:    The requests session to use
            auth:       The Python requests authoriser to use to authorise the API requests.
        """
        self._base_url = base_url
        
        if session is None:
            session = requests.Session()
            session.auth = auth
        
        self.session = session
    
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
        return self._fetch_resource(id, model.BaseImage)
    
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
        return self._fetch_resource(id, model.Model)
    
    def get_models(self, skip=None, limit=None, page_size=None, group_ids=None):
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
        return self._get_resources(model.Model, skip, limit, page_size, groupids=group_ids)
    
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
        return self._fetch_resource(id, model.Workflow)
    
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
    
    def upload_workflow(self, workflow):
        """
        Upload a workflow to the analysis service. If the workflow specifies an
        ID matching an existing workflow, the existing workflow is overwritten.
        If no ID is specified, a new workflow is created with an automatically
        generated ID.
        
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
        return self._upload_resource(workflow)
    
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
                response = self.session.get(url=url, params=params)
            
            # ...otherwise, if ID is unknown or workflow doesn't exist, create it.
            if workflow.id is None or response.status_code == 404:
                workflow = self.post_workflow(workflow).id
        
        # If by this point "workflow" is an ID string (not a Workflow instance),
        # either by being passed in a such or as retrieved when creating the
        # workflow, run the workflow with the given ID.
        if isinstance(workflow, (str, basestring)):
            url = util.append_path_to_url(self._base_url, 'workflows', workflow, 'results')
            response = self.session.get(url=url, params=params)
        
        return model.WorkflowResults(self, self._check_response(response))
    
    def delete_workflow(self, workflow):
        workflow_id = getattr(workflow, 'id', workflow)
        
        url = util.append_path_to_url(self._base_url, 'workflows', workflow_id)
        self._check_response(self.session.delete(url), False)
    
    def create_job(self, workflow, debug=False):
        """
        Create a workflow execution job.
        
        Args:
            workflow: The ID of the workflow to execute, or a Workflow instance
                describing the workflow to execute.
            debug: If true, the workflow is run in "debug" mode (which causes
                additional log messages and output data to be returned in the
                response).
        
        Returns:
            An new instance of the Job class.
        
        Raises:
            RequestError: if an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: if an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        if isinstance(workflow, model.Workflow) and workflow.id is None:
            workflow = self.post_workflow(workflow)
        if isinstance(workflow, model.Workflow):
            workflow = workflow.id
        
        return self._post_resource(model.Job(workflow, debug))
    
    def get_job(self, job):
        return self._fetch_resource(job, model.Job)
    
    def _fetch_resource(self, resource, type_=None):
        updating = not isinstance(resource, (str,basestring))
        
        assert updating or type_ is not None
        
        if updating:
            type_ = type(resource)
        
        assert hasattr(type_, '_url_path')
        assert callable(getattr(type_, '_update', None))
        
        id_ = resource.id if updating else resource
        url = util.append_path_to_url(self._base_url, type_._url_path, id_)
        json = self._check_response(self.session.get(url=url))
        
        instance = resource if updating else type_()
        return instance._update(self, json)
    
    def _get_resources(self, type_, skip, limit, page_size, **kwargs):
        assert hasattr(type_, '_url_path')
        assert hasattr(type_, '_collection')
        assert callable(getattr(type_, '_update', None))
        
        if skip is None and limit is None and not kwargs:
            return model._ResourceCollection(self, type_, page_size)
        elif page_size is not None:
            raise ValueError('The "page_size" parameter cannot be used if the "skip" or "limit" parameters are used.')
        else:
            query = { 'skip': skip, 'limit': limit }
            
            for k,v in kwargs.iteritems():
                if isinstance(v, collections.Sequence) and not isinstance(v, (str, basestring)):
                    query[k] = ','.join(str(sv) for sv in v)
                elif v is not None:
                    query[k] = str(v)
            
            url = util.append_path_to_url(self._base_url, type_._url_path)
            json = self._check_response(self.session.get(url=url, params=query))
            
            return model._ResourceList(self, type_, json)
    
    def _post_resource(self, resource):
        return self._upload_resource(resource, method='POST')
    
    def _upload_resource(self, resource, method=None):
        assert hasattr(type(resource), '_url_path')
        assert callable(getattr(resource, '_serialise', None))
        
        if method == 'PUT' and not resource.id:
            raise ValueError('Cannot PUT resource without an ID.')
        elif method is None:
            method = 'PUT' if resource.id else 'POST'
        
        assert method in ('PUT', 'POST')
        
        putting = (method == 'PUT')
        
        resource_json = resource._serialise(include_id=putting)
        path_parts = [resource.__class__._url_path]
        if putting:
            path_parts.append(resource.id)
        url = util.append_path_to_url(self._base_url, *path_parts)
        
        resource_json = self._check_response(self.session.request(method, url, json=resource_json))
        
        return resource._update(self, resource_json)
    
    def _post_model_archive(self, archive_file, name, mime_type):
        url = util.append_path_to_url(self._base_url, 'models')
        logger.debug('Uploading new model to %s...', url)
        files = { 'archive': (name, archive_file, mime_type, {}) }
        
        response = self.session.post(url=url, files=files)
        logger.log(TRACE, 'Response: %s', response.text)
        
        return model.ModelInstallationResult(self, self._check_response(response))
    
    def _check_response(self, response, expect_json=True):
        if 400 <= response.status_code < 500:
            raise exceptions.RequestError(response, **response.json())
        elif 500 <= response.status_code:
            raise exceptions.ServerError(response, **response.json())
        
        if expect_json:
            return response.json()
    
    base_url = property(lambda self: self._base_url)
