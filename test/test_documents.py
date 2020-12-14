
from as_client import Client, Document, RequestError
import dotenv
import os
import requests
import unittest
import vcr


class DocumentTests(unittest.TestCase):
    def setUp(self):
        dotenv.load_dotenv()

        session = requests.Session()
        session.params = {'apikey': os.getenv('SENAPS_API_KEY')}

        self.client = Client(os.getenv('ANALYSIS_SERVICE_URL_BASE'), session)

    @vcr.use_cassette('cassettes/documents/get_document.yaml')
    def test_get_document(self):
        document_id = 'a8f55cb0-62e7-4757-8da1-9492368b44b9'
        document = self.client.get_document(document_id)

        self.assertEqual(document.id, document_id)
        self.assertTrue(document.value.lower().startswith('lorem ipsum dolor sit amet'))

    @vcr.use_cassette('cassettes/documents/get_document_value.yaml')
    def test_get_document_value(self):
        self.the_document_value_matches_local_file('a8f55cb0-62e7-4757-8da1-9492368b44b9',
                                                   'a8f55cb0-62e7-4757-8da1-9492368b44b9.txt')

    @vcr.use_cassette('cassettes/documents/create_new_document.yaml')
    def test_create_new_document(self):
        document_id = '1a766dda-6914-4446-9488-3ae6909930e4'
        organisation_id = 'csiro'

        self.the_document_does_not_exist(document_id)
        self.set_document_from_local_file(document_id, '1a766dda-6914-4446-9488-3ae6909930e4.txt',
                                          organisation_id='csiro')
        self.the_document_value_matches_local_file(document_id, '1a766dda-6914-4446-9488-3ae6909930e4.txt')

    @vcr.use_cassette('cassettes/documents/update_existing_document.yaml')
    def test_update_document(self):
        document_id = '77e7df34-8f9b-4f95-8eee-fc62ec70f37c'

        document = self.client.get_document(document_id)
        self.assertEqual(document.id, document_id)
        self.assertFalse(document.value.lower().startswith('lorem ipsum dolor sit amet'))

        updated_document = self.set_document_from_local_file(document, '77e7df34-8f9b-4f95-8eee-fc62ec70f37c.txt')
        self.assertEqual(updated_document.id, document_id)
        self.assertEqual(updated_document.organisation_id, document.organisation_id)
        self.assertEqual(updated_document.group_ids, document.group_ids)

        self.the_document_value_matches_local_file(document_id, '77e7df34-8f9b-4f95-8eee-fc62ec70f37c.txt')

    def the_document_does_not_exist(self, document_or_id):
        with self.assertRaises(RequestError) as context:
            self.client.get_document(document_or_id)

        self.assertEqual(context.exception.status_code, 404)

    def set_document_from_local_file(self, document_or_id, local_file_name, organisation_id=None):
        document = self.client.set_document_value(document_or_id,
                                                  path=os.path.join('resources', 'documents', local_file_name),
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
        with open(os.path.join('resources', 'documents', local_file_name)) as f:
            expected_value = f.read()

        actual_value = self.client.get_document_value(document_or_id)

        self.assertEqual(expected_value.rstrip(), actual_value.rstrip())
