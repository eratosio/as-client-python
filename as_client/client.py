
import requests

import json, logging, os, posixpath, tempfile, zipfile

logger = logging.getLogger(__name__)
TRACE = 5

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
				or to a ZIP file containing the files.

			manifest (dict, optional): The model's manifest.
			
				If omitted, the given directory or ZIP file MUST contain a
				manifest.json file containing the model's manifest.
		
		Raises:
			RequestError: if an HTTP "client error" (4XX) status code is
				returned by the server.
			ServerError: if an HTTP "server error" (5XX) status code is returned
				by the server.
		"""
		if os.path.isdir(path):
			logger.debug('Generating new model zip file from files at path %s', path)
			with tempfile.TemporaryFile() as f:
				with zipfile.ZipFile(f, 'w') as zip_file:
					for root, dirs, files in os.walk(path):
						for file_ in files:
							source_path = os.path.join(root, file_)
							dest_path = os.path.relpath(source_path, path)
							
							if manifest is None or dest_path != 'manifest.json':
								zip_file.write(source_path, arcname=dest_path)
					
					if manifest is not None:
						zip_file.writestr('manifest.json', json.dumps(manifest))
				
				f.seek(0)
				self._post_model_archive(f)
		elif zipfile.is_zipfile(path):
			logger.debug('Uploading model zip file %s', path)
			with open(path, 'rb') as f:
				self._post_model_archive(f)
		else:
			raise ValueError('Path {} does not refer to a directory or zip file.'.format(path))
	
	
	def _post_model_archive(self, zip_file):
		url = posixpath.join(self._base_url, 'models')
		logger.debug('Uploading new model to %s...', url)
		files = { 'archive': ('model.zip', zip_file, 'application/zip', {}) }
		
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
