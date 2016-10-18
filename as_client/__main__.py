
import as_client

import requests

import argparse, json, os

def install_model(client, args):
	model_path = os.path.abspath(args['model'])
	print 'Installing model from {}'.format(model_path)
	
	# Get manifest file, if specified.
	manifest = None
	if 'manifest' in args:
		manifest_path = os.path.abspath(args['manifest'])
		print 'Overriding manifest with content of {}'.format(manifest_path)
		
		with open(manifest_path, 'r') as f:
			manifest = json.load(f)
	
	client.install_model(model_path, manifest)

# Create top-level arg parser.
parser = argparse.ArgumentParser(description='Analysis Services API client')
parser.add_argument('api_base', help='TODO')
subparsers = parser.add_subparsers(help='sub-command help')

# Create parser for common options.
opts_parser = argparse.ArgumentParser(add_help=False)
opts_parser.add_argument('--username', help='Username to authenticate with', default=argparse.SUPPRESS)
opts_parser.add_argument('--password', help='Password to authenticate with', default=argparse.SUPPRESS)
opts_parser.add_argument('--apikey', help='API key to authenticate with', default=argparse.SUPPRESS)

# create the parser for the "install_model" command
install_model_parser = subparsers.add_parser('install_model', help='TODO', parents=[opts_parser])
install_model_parser.add_argument('model', help='The path to the model (either a zip file, or the directory containing the model files).')
install_model_parser.add_argument('--manifest', help='The path to the manifest file.', default=argparse.SUPPRESS)
install_model_parser.set_defaults(func=install_model)

# Parse command line.
namespace = parser.parse_args()
args = vars(namespace)

# Generate API authorisation object.
if 'apikey' in args:
	print 'Not implemented' # TODO
	exit(0)
elif 'username' in args and 'password' in args:
	auth = (args['username'], args['password'])
else:
	auth = None

# Initialise API client.
client = as_client.Client(args['api_base'], auth)

# Run the selected command.
namespace.func(client, args)
