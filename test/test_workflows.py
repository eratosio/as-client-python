
from as_client import Client, Document, RequestError
import json
import os
import posixpath
import re
import requests
import responses
import unittest
from urllib.parse import urlparse, urlunparse

from as_client.model import CollectionNode, DocumentNode


def url_path_join(url, *paths):
    url_parts = list(urlparse(url))
    url_parts[2] = posixpath.join(url_parts[2], *paths)
    return urlunparse(url_parts)


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        base_url = 'http://test.senaps.io/api/analysis/'
        self.workflows_url = url_path_join(base_url, 'workflows')

        self._workflows = {}

        self.responses = responses.RequestsMock()

        self.responses.add_callback(responses.GET, re.compile(url_path_join(self.workflows_url, '[^/]+')),
                                    self._get_workflow)
        
        self.addCleanup(self.responses.stop)
        self.addCleanup(self.responses.reset)
        self.responses.start()

        session = requests.Session()
        session.params = {'apikey': 'test_api_key'}

        self.client = Client(base_url, session)
    
    @staticmethod
    def _get_workflow_folder(workflow_id):
        return os.path.join('test', 'resources', 'workflows', workflow_id)

    def test_get_workflow(self):
        workflow_id = 'eca25e91-e0c6-4099-8045-fdc4782fbd9a'

        self.with_the_test_workflow(workflow_id)

        workflow = self.client.get_workflow(workflow_id)

        self.assertEqual(workflow.id, workflow_id)
        
        # Check that the workflow has correctly parsed the collection
        collection_nodes = list(filter(lambda x: isinstance(x, CollectionNode), workflow.graph.nodes))
        self.assertEqual(len(collection_nodes), 3)

        for node in collection_nodes:
            self.assertTrue(hasattr(node, 'collection'), f'Collection node {node.id} does not have a collection')
            # Check that in our case each collection is made up of 1 document
            self.assertEqual(len(node.collection), 1)
            self.assertTrue(isinstance(node.collection[0], DocumentNode), f'Collection node {node.id} does not contain a document')
            # And that the document does not have an ID, but instead just has a document ID
            self.assertTrue(node.collection[0].document_id is not None, f'Collection node {node.id} contains a document without a document id')
            self.assertTrue(node.collection[0].id is None, f'Collection node {node.id} contains a document with an id')

    def with_the_test_workflow(self, workflow_id):
        with open(os.path.join(WorkflowTests._get_workflow_folder(workflow_id), "workflow.json"), 'r') as f:
            self._workflows[workflow_id] = json.load(f)

    def with_the_workflow(self, workflow):
        workflow_id = workflow['id']
        self._workflows[workflow_id] = workflow

    def _get_workflow(self, request):
        return self._make_workflow_response(self._get_id_from_url(request.url, self.workflows_url))

    def _put_workflow(self, request):
        workflow_id = self._get_id_from_url(request.url, self.workflows_url)
        self._workflows[workflow_id] = json.loads(request.body)

        return self._make_workflow_response(workflow_id)

    def _make_workflow_response(self, workflow_id):
        try:
            workflow = dict(self._workflows[workflow_id])  # NOTE: copy to prevent mutation of original
        except KeyError:
            return 404, {}, '{"statuscode": 404}'

        return 200, {'Content-Type': 'application/json'}, json.dumps(workflow)

    def _get_id_from_url(self, url, base_url):
        url_path = urlparse(url).path
        base_path = urlparse(base_url).path

        return posixpath.relpath(url_path, base_path).split(posixpath.sep)[0]

if __name__ == "__main__":
    unittest.main()