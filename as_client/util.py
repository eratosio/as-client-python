
import os, posixpath, warnings

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

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

def append_path_to_url(url, *args):
    parts = list(urlparse.urlparse(url))
    parts[2] = posixpath.join(parts[2], *args)
    return urlparse.urlunparse(parts)
