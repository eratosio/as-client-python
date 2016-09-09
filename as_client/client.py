
import requests

import json, os, tempfile, urlparse, zipfile

class Error(Exception):
	def __init__(self, message, statuscode):
		super(Error, self).__init__(message)
		
		self.status_code = statuscode

class RequestError(Error):
	pass

class ServerError(Error):
	pass

class Client(object):
	def __init__(self, base_url, auth):
		self._base_url = base_url
		
		self._session = requests.Session()
		self._session.auth = auth
	
	def install_model(self, path, manifest=None):
		if os.path.isdir(path):
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
			with open(path, 'rb') as f:
				self._post_model_archive(f)
		else:
			raise ValueError('Path {} does not refer to a directory or zip file.'.format(path))
	
	
	def _post_model_archive(self, zip_file):
		url = urlparse.urljoin(self._base_url, '/models')
		files = { 'archive': ('model.zip', zip_file, 'application/zip', {}) }
		
		response = self._session.post(url=url, files=files)
		print response.text
		
		if 400 <= response.status_code < 500:
			raise RequestError(**response.json())
		elif 500 <= response.status_code:
			raise ServerError(**response.json())
		
		print response # TODO: handle valid response
