
import os, warnings

if os.name == 'posix':
	def path_is_hidden(path):
		drive, path = os.path.splitdrive(path)
		while path:
			new_path, tail = os.path.split(path)
			if tail.startswith('.'):
				return True
			elif new_path == path:
				return False
			
			path = new_path
		
		return False
else:
	def path_is_hidden(path):
		warnings.warn('path_is_hidden() is not specialised for OS "{}" - all files will be considered non-hidden')
		return False
