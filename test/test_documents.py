
from as_client import Client, Document, RequestError
import json
import os
import posixpath
import re
import requests
import responses
import unittest
from urllib.parse import urlparse, urlunparse


def url_path_join(url, *paths):
    url_parts = list(urlparse(url))
    url_parts[2] = posixpath.join(url_parts[2], *paths)
    return urlunparse(url_parts)


class DocumentTests(unittest.TestCase):
    def setUp(self):
        base_url = 'http://test.senaps.io/api/analysis/'
        self.documents_url = url_path_join(base_url, 'documentnodes')

        self._documents = {}

        self.responses = responses.RequestsMock()

        self.responses.add_callback(responses.GET, re.compile(url_path_join(self.documents_url, '[^/]+', 'value')),
                                    self._get_value)
        self.responses.add_callback(responses.GET, re.compile(url_path_join(self.documents_url, '[^/]+')),
                                    self._get_document)
        self.responses.add_callback(responses.PUT, re.compile(url_path_join(self.documents_url, '[^/]+')),
                                    self._put_document)

        self.addCleanup(self.responses.stop)
        self.addCleanup(self.responses.reset)
        self.responses.start()

        session = requests.Session()
        session.params = {'apikey': 'test_api_key'}

        self.client = Client(base_url, session)

    @staticmethod
    def _get_document_folder(document_id):
        return os.path.join('test', 'resources', 'documents', document_id)

    def test_get_document(self):
        document_id = 'a8f55cb0-62e7-4757-8da1-9492368b44b9'

        self.with_the_test_document(document_id)

        document = self.client.get_document(document_id)

        self.assertEqual(document.id, document_id)
        self.assertTrue(document.value.lower().startswith('lorem ipsum dolor sit amet'))

    def test_get_document_value(self):
        document_id = 'a8f55cb0-62e7-4757-8da1-9492368b44b9'

        self.with_the_test_document(document_id)

        self.the_document_value_matches_local_file(document_id, 'a8f55cb0-62e7-4757-8da1-9492368b44b9/value.txt')

    def test_create_new_document(self):
        document_id = '1a766dda-6914-4446-9488-3ae6909930e4'

        self.the_document_does_not_exist(document_id)
        self.set_document_from_local_file(document_id, '1a766dda-6914-4446-9488-3ae6909930e4/value.txt',
                                          organisation_id='csiro')
        self.the_document_value_matches_local_file(document_id, '1a766dda-6914-4446-9488-3ae6909930e4/value.txt')

    def test_update_document(self):
        document_id = '77e7df34-8f9b-4f95-8eee-fc62ec70f37c'

        self.with_the_document({
            'documentid': document_id,
            'value': 'initial_value',
            'organisationid': 'csiro',
            'groupids': ['test_group']
        })

        document = self.client.get_document(document_id)
        self.assertEqual(document.id, document_id)
        self.assertFalse(document.value.lower().startswith('lorem ipsum dolor sit amet'))

        updated_document = self.set_document_from_local_file(document, '77e7df34-8f9b-4f95-8eee-fc62ec70f37c/value.txt')
        self.assertEqual(updated_document.id, document_id)
        self.assertEqual(updated_document.organisation_id, document.organisation_id)
        self.assertEqual(updated_document.group_ids, document.group_ids)

        self.the_document_value_matches_local_file(document_id, '77e7df34-8f9b-4f95-8eee-fc62ec70f37c/value.txt')

    def the_document_does_not_exist(self, document_or_id):
        with self.assertRaises(RequestError) as context:
            self.client.get_document(document_or_id)

        self.assertEqual(context.exception.status_code, 404)

    def set_document_from_local_file(self, document_or_id, local_file_name, organisation_id=None):
        document = self.client.set_document_value(document_or_id,
                                                  path=DocumentTests._get_document_folder(local_file_name),
                                                  organisation_id=organisation_id)

        if isinstance(document_or_id, Document):
            document_id = document_or_id.id

            if organisation_id is None:
                organisation_id = document_or_id.organisation_id
        else:
            document_id = document_or_id

        self.assertEqual(document.id, document_id)
        self.assertEqual(document.organisation_id, organisation_id)

        return document

    def the_document_value_matches_local_file(self, document_or_id, local_file_name):
        with open(DocumentTests._get_document_folder(local_file_name)) as f:
            expected_value = f.read()

        actual_value = self.client.get_document_value(document_or_id)

        self.assertEqual(expected_value.rstrip(), actual_value.rstrip())

    def with_the_test_document(self, document_id):
        document_folder = DocumentTests._get_document_folder(document_id)

        with open(os.path.join(document_folder, 'document.json'), 'r') as f:
            document = self._documents[document_id] = json.load(f)
        with open(os.path.join(document_folder, 'value.txt'), 'r') as f:
            document['value'] = f.read()

    def with_the_document(self, document):
        document_id = document['documentid']

        self._documents[document_id] = document

    def _get_document(self, request):
        return self._make_document_response(self._get_document_id_from_url(request.url))

    def _get_value(self, request):
        document_id = self._get_document_id_from_url(request.url)

        try:
            return 200, {'Content-Type': 'application/json'}, self._documents[document_id]['value']
        except KeyError:
            return 404, {}, '{"statuscode": 404}'

    def _put_document(self, request):
        document_id = self._get_document_id_from_url(request.url)
        self._documents[document_id] = json.loads(request.body)

        return self._make_document_response(document_id)

    def _make_document_response(self, document_id):
        try:
            document = dict(self._documents[document_id])  # NOTE: copy to prevent mutation of original
        except KeyError:
            return 404, {}, '{"statuscode": 404}'

        truncated = document['valuetruncated'] = len(document['value']) > 1024
        if truncated:
            document['value'] = document['value'][:1024]

        return 200, {'Content-Type': 'application/json'}, json.dumps(document)

    def _get_document_id_from_url(self, url):
        url_path = urlparse(url).path
        base_path = urlparse(self.documents_url).path

        return posixpath.relpath(url_path, base_path).split(posixpath.sep)[0]
