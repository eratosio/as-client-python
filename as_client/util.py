
import os, warnings

def is_gz_file(path):
	# Test file extension.
	path, ext = os.path.splitext(path)
	if ext.lower() not in {'.gz', '.tgz'}:
		return False
	
	# Test magic number.
	with open(path, 'rb') as f:
		if f.read(2) != '\x1f\x8b':
			return False
	
	return True

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
