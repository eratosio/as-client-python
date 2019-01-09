
from __future__ import print_function

import as_client

import requests

import argparse, errno, getpass, json, os

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse # Moved in Python 3

try:
    input = raw_input # raw_input() became input() in Python 3
except NameError:
    pass

system_config_file = '/etc/as-client/config.json'
user_config_dir = os.path.expanduser('~/.as-client')
user_config_file = os.path.join(user_config_dir, 'config.json')

def load_configs():
    system_config = user_config = {}

    # Attempt to load the system config file (if any).
    try:
        with open(system_config_file) as f:
            system_config = json.load(f)
    except IOError:
        pass

    # Attempt to load user's personal config file (if any).
    try:
        with open(user_config_file) as f:
            user_config = json.load(f)
    except IOError:
        pass

    merged_config = {
        'hosts': dict(system_config.get('hosts', {}), **user_config.get('hosts', {})),
        'defaultHost': system_config.get('defaultHost', user_config.get('defaultHost', None))
    }

    return system_config, user_config, merged_config

def authmode_for_host(host):
    try:
        return host['authmode']
    except KeyError:
        have_key_auth = bool(host.get('apikey', None))
        have_user_auth = host.get('username', None) or host.get('password', None)

        if have_key_auth and not have_user_auth:
            return 'key'
        elif not have_key_auth and have_user_auth:
            return 'user'

def resolve_host(args):
    # Load configurations.
    system_config, user_config, merged_config = load_configs()

    # Get the host selected on the command line (if any).
    # If no host specified and no default host exists, use an empty host.
    host_id = args.get('host', '$default')
    if host_id == '$default':
        host_id = merged_config['defaultHost']
    host = merged_config['hosts'].get(host_id, {})

    # Any URL from command line overrides one in the host configuration.
    if 'url' in args:
        host['url'] = args['url']

    # If URL still not known, prompt user.
    if not host.get('url', None):
        host['url'] = input('Please enter API base URL: ')

    # Attempt to extract any username, password and/or API key contained within
    # the URL. Extracted credentials DO NOT override credentials already present
    # in the host configuration.
    # TODO: api key
    parts = urlparse.urlparse(host['url'])
    host.setdefault('username', parts.username)
    host.setdefault('password', parts.password)
    netloc = parts.netloc.rpartition('@')[2] # Strip credentials
    host['url'] = urlparse.urlunparse((parts.scheme, netloc, parts.path, parts.params, parts.query, parts.fragment))

    # Credentials supplied on command line override those from any other source.
    auth_keys = ('apikey', 'username', 'password')
    host.update({ k:args[k] for k in args if k in auth_keys })

    # Determine auth mode to use, prompting if necessary.
    host['authmode'] = authmode_for_host(host)
    while not host.get('authmode', None):
        host['authmode'] = { '1': 'none', '2': 'user', '3': 'key' }.get(input('Please select authentication method (1 for none, 2 for username/password, 3 for API key) [default=1]: ') or '1', None)

    # Prompt for any missing authentication credentials.
    url = host['url']
    username = getpass.getuser()
    while (host['authmode'] == 'key') and not host.get('apikey', ''):
        host['apikey'] = input('Please enter API key for {}: '.format(url))
    if (host['authmode'] == 'user') and not host.get('username', ''):
        host['username'] = input('Please enter username for {} [default={}]: '.format(url, username)) or username
    while (host['authmode'] == 'user') and not host.get('password', ''):
        host['password'] = getpass.getpass('Please enter password for user {} at {}: '.format(host['username'], url))

    # Remove redundant members.
    if host['authmode'] in ('key', 'none'):
        host.pop('username', None)
        host.pop('password', None)
    elif host['authmode'] in ('user', 'none'):
        host.pop('apikey', None)

    return host

def install_model(client, args, host_config):
    model_path = os.path.abspath(args['model'])
    print('Installing model from {}'.format(model_path))

    # Get manifest file, if specified.
    manifest = None
    if 'manifest' in args:
        manifest_path = os.path.abspath(args['manifest'])
        print('Overriding manifest with content of {}'.format(manifest_path))

        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

    client.install_model(model_path, manifest)

# Create top-level arg parser.
parser = argparse.ArgumentParser(description='Analysis Services API client')
subparsers = parser.add_subparsers(help='sub-command help')

# Create parser for common options.
opts_parser = argparse.ArgumentParser(add_help=False)
opts_parser.add_argument('-H', '--host', help='Name of stored host configuration to use.', default=argparse.SUPPRESS)
opts_parser.add_argument('-U', '--url', help='API\' base URL.', default=argparse.SUPPRESS)
opts_parser.add_argument('-u', '--username', help='Username to authenticate with', default=argparse.SUPPRESS)
opts_parser.add_argument('-p', '--password', help='Password to authenticate with', default=argparse.SUPPRESS)
opts_parser.add_argument('-a', '--apikey', help='API key to authenticate with', default=argparse.SUPPRESS)

# create the parser for the "install_model" command
install_model_parser = subparsers.add_parser('install_model', help='TODO', parents=[opts_parser])
install_model_parser.add_argument('model', help='The path to the model (either a zip file, or the directory containing the model files).')
install_model_parser.add_argument('-m', '--manifest', help='The path to the manifest file.', default=argparse.SUPPRESS)
install_model_parser.set_defaults(func=install_model)

# Parse command line.
namespace = parser.parse_args()
args = vars(namespace)

# Resolve host configuration.
host_config = resolve_host(args)

# Generate API authorisation object.
if host_config['authmode'] == 'key':
    print('Not implemented') # TODO
    exit(0)
if host_config['authmode'] == 'user':
    auth = (host_config['username'], host_config['password'])
else:
    auth = None

# Initialise API client.
client = as_client.Client(host_config['url'], auth)

# Run the selected command.
namespace.func(client, args, host_config)
