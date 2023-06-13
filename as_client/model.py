
from __future__ import division, print_function

import collections, inspect

_UNKNOWN = object()  # Placeholder for indicating an unknown value (since None might be a valid value).

################################################################################
# Resource property classes.                                                   #
################################################################################


class _Property(object):
    """
    This class implements a data descriptor for the client's "resource" classes
    (see _Resource below).

    Specifically, this descriptor is used to manage the resource's attributes
    that are included in the JSON payload from the API. It includes
    functionality for extracting the attribute's value from a JSON document,
    providing a default value if the attribute is not included in the JSON
    document, triggering a fetch of the resource if the attribute hasn't been
    retrieved previously, and tracking changes made to the attribute's value on
    the client side (where permitted).
    """
    def __init__(self, json_name, from_json=lambda v: v, to_json=lambda v: v, default=None, writable=False,
                 serialize=True, serialise_as=None):
        self.json_name = json_name
        self.from_json = from_json
        self.to_json = to_json
        self.default = default
        self.writable = writable
        self.serialize = serialize
        self.serialise_as = json_name if serialise_as is None else serialise_as

    def __get__(self, instance, owner):
        if instance is None:
            return self

        data = self.get_data(instance)

        if data.needs_fetch:
            id_ = getattr(instance, 'id', None)
            client = getattr(instance, '_client', None)
            if None not in (id_, client):
                client._fetch_resource(instance, owner)

        return data.local_value

    def __set__(self, instance, value):
        if not self.writable:
            cls = instance.__class__
            descriptors = inspect.getmembers(cls, lambda m: m is self)
            assert len(descriptors) == 1

            raise AttributeError('Property "{}" of class "{}" is not writable.'.format(descriptors[0][0], cls.__name__))

        self.set_value(instance, value)

    def __delete__(self, instance):
        pass  # TODO

    def set_value(self, instance, value):
        self.get_data(instance).local_value = value

    def get_data(self, instance):
        try:
            props = instance.__properties
        except AttributeError:
            props = instance.__properties = {}

        return props.setdefault(self.json_name, _PropertyData(self))

    def _update(self, instance, json):
        if self.json_name in json:
            self.get_data(instance)._remote_value = self.from_json(json[self.json_name])

    def _serialise(self, instance, json):
        if not self.serialize:
            return

        local_value = self.get_data(instance).local_value
        if local_value is not _UNKNOWN:
            json[self.serialise_as] = self.to_json(local_value)


class _IdProperty(_Property):
    """
    A subclass of the _Property data descriptor.

    This descriptor only differs from the standard _Property descriptor in that
    it *doesn't* trigger a fetch of the resource if the value hasn't previously
    been retrieved - for example, if we don't know the resource's ID, there's no
    point trying to fetch it since the ID is needed to resolve the resource's
    URL.
    """
    def __get__(self, instance, owner):
        return self if instance is None else self.get_data(instance).local_value


class _EmbeddedProperty(_Property):
    """
    A subclass of the _Property data descriptor.

    This descriptor differs from the standard _Property descriptor in that when
    parsing JSON, it extracts the property value from the JSON object's
    "_embedded" property (if present) rather than from the JSON object itself.
    This is used to support parsing of Hypertext Application Language payloads.

    TODO: update to support arbitrary properties of the JSON object, not just
    "_embedded"?
    """
    def _update(self, instance, json):
        json = json.get('_embedded', {})

        if self.json_name in json:
            self.get_data(instance)._remote_value = self.from_json(json[self.json_name])

    """def _serialise(self, instance, json):
        local_value = self.get_data(instance).local_value
        if local_value is not _UNKNOWN:
            json.setdefault('_embedded', {})[self.json_name] = self.to_json(local_value)"""


class _PropertyData(object):
    """
    A class for storing property values.

    This class is primarily concerned with tracking both a "local" and a
    "remote" version of the property's value, to allow changes made locally to
    be tracked.
    """
    def __init__(self, property_):
        self._property = property_

        self._local_value = self._remote_value = _UNKNOWN

    @property
    def local_value(self):
        if self._local_value is not _UNKNOWN:
            return self._local_value
        elif self._remote_value is not _UNKNOWN:
            return self._remote_value
        else:
            return self._property.default

    @local_value.setter
    def local_value(self, value):
        self._local_value = value

    @property
    def needs_fetch(self):
        return self._remote_value is _UNKNOWN

################################################################################
# Resource base classes.                                                       #
################################################################################


class _Resource(object):
    """
    Base class for all the client's "resource" classes.
    """

    def _update(self, client, json):
        self._client = client

        for prop_id, prop in inspect.getmembers(self.__class__, lambda m: isinstance(m, _Property)):
            prop._update(self, json)

        return self

    def _serialise(self, include_id=True):
        result = {}

        for prop_id, prop in inspect.getmembers(self.__class__, lambda m: isinstance(m, _Property)):
            if not include_id and isinstance(prop, _IdProperty):
                continue

            prop._serialise(self, result)

        return result

    def copy(self):
        type_ = self.__class__
        copy = type_()

        for prop_id, prop in inspect.getmembers(self.__class__, lambda m: isinstance(m, _Property)):
            prop.set_value(copy, prop.__get__(self, type_))

        setattr(copy, '_client', getattr(self, '_client', None))

        return copy



class _ResourceList(collections.abc.Sequence):
    """
    A thin wrapper around a list of resource class instances.

    Given a JSON object representing a HAL-encapsulated list of resources,
    deserialises the resource instances into a list and extracts the associated
    paging metadata.
    """
    def __init__(self, client, type_, json):
        self._type = type_

        self._items = [type_()._update(client, v) for v in json.get('_embedded', {}).get(type_._collection, [])]
        self._skip = json.get('skip')
        self._limit = json.get('limit')
        self._count = json.get('count')
        self._total_count = json.get('totalcount')

    def __getitem__(self, index):
        return self._items[index]

    def __len__(self):
        return len(self._items)

    skip = property(lambda self: self._skip)
    limit = property(lambda self: self._limit)
    count = property(lambda self: self._count)
    total_count = property(lambda self: self._total_count)

class _ResourceCollection(collections.abc.Sequence):
    """
    Implementation of a sequence of resource instances that transparently
    handles paging of requests to the API.
    """
    def __init__(self, client, type_, page_size):
        self._client = client
        self._type = type_
        self._page_size = page_size

        self._items = self._length = None

    def __getitem__(self, index):
        try:
            # If item not previously loaded, load it now.
            if not self._is_item_loaded(index):
                # If page size unknown, load the first page to find out the default
                # page size.
                if self._page_size is None:
                    self._load(0)

                # If item still not loaded, load its page.
                if not self._is_item_loaded(index):
                    self._load((index // self._page_size) * self._page_size)

            result = self._items[index]
            assert result is not _UNKNOWN

            return result
        except IndexError:
            raise IndexError('Index {} out of range for resource collection with {} items.'.format(index, self._length))

    def __len__(self):
        if self._length is None:
            self._load(0)
        return self._length

    def _load(self, skip):
        items = self._client._get_resources(self._type, skip, self._page_size, None)

        self._length = items.total_count
        self._page_size = items.limit

        if self._items is None:
            self._items = [_UNKNOWN] * self._length

        self._items[items.skip:items.skip+items.count] = items

    def _is_item_loaded(self, index):
        return (self._items is not None) and (self._items[index] is not _UNKNOWN)

################################################################################
# Resource component classes.                                                  #
################################################################################

class HostEnvironment(object):
    """
    Representation of the "host environment" type used in the "base image"
    type's "hostenvironment" property.

    Attributes:
        architecture: A string describing the host's CPU architecture (e.g. "X86_32", "X86_64").
        operating_system: A string describing the host's operating system (e.g. LINUX, WINDOWS, MAC_OS).
    """
    def __init__(self, json):
        self.architecture = json.get('architecture')
        self.operating_system = json.get('operatingsystem')

    def _serialise(self):
        return {
            'architecture': self.architecture,
            'operatingsystem': self.operating_system
        }

class Graph(object):
    """

    """
    def __init__(self, json={}):
        json = json.get('_embedded', {})

        nodes = [_GraphNode._deserialise(v) for v in json.get('nodes', [])]
        nodes = { n.id: n for n in nodes }
        self.nodes = list(nodes.values())

        self.connections = [
            GraphConnection(
                nodes[c['source']['node']],
                nodes[c['target']['node']],
                c['source'].get('port'),
                c['target'].get('port')
            ) for c in json.get('connections', [])
        ]

    def clone(self):
        result = Graph()

        result.nodes = self.nodes.copy()
        result.connections = list(self.connections)

        return result

    def _serialise(self):
        return {
            'nodes': [n._serialise() for n in self.nodes],
            'connections': [c._serialise() for c in self.connections]
        }

class _GraphNode(object):
    def __init__(self, id, label):
        self.id = id
        self.label = label

    def _serialise(self):
        return { 'id': self.id, 'label': self.label }

    @staticmethod
    def _validate_id(json, in_collection=False):
        if json.get("id") is None and not in_collection:
            raise ValueError("Node ID must be specified.")

    @staticmethod
    def _deserialise(json, in_collection=False):

        if "collection" in json.get("_embedded", {}):
            return CollectionNode._deserialise(json)

        # Ensure that the node has an ID if we're not in a collection
        _GraphNode._validate_id(json, in_collection)

        if 'modelid' in json:
            return ModelNode._deserialise(json)
        elif 'documentid' in json or 'value' in json:
            return DocumentNode._deserialise(json)
        elif 'streamid' in json:
            return StreamNode._deserialise(json)
        elif 'streamids' in json:
            return MultiStreamNode._deserialise(json)
        elif 'dataset' in json:
            return GridNode._deserialise(json)
        else:
            raise ValueError(f"Unrecognised node type {json}")

class ModelNode(_GraphNode):
    def __init__(self, id, label, model_id):
        super(ModelNode, self).__init__(id, label)

        self.model_id = model_id

    def _serialise(self):
        return dict(super(ModelNode, self)._serialise(), modelid=self.model_id)

    @staticmethod
    def _deserialise(json):
        return ModelNode(json.get("id"), json.get('label'), json['modelid'])

class DocumentNode(_GraphNode):
    def __init__(self, id, label, value=None, document_id=None):
        super(DocumentNode, self).__init__(id, label)

        if (value is None) and (document_id is None):
            raise ValueError('At least one of the value or document ID may be specified.')

        self.value = value
        self.document_id = document_id

    def _serialise(self):
        result = super(DocumentNode, self)._serialise()
        if self.value is not None:
            result['value'] = self.value
        if self.document_id is not None:
            result['documentid'] = self.document_id
        return result

    @staticmethod
    def _deserialise(json):
        return DocumentNode(json.get("id"), json.get('label'), json.get('_embedded', {}).get('datanode', {}).get('value'), json.get('documentid'))

class StreamNode(_GraphNode):
    def __init__(self, id, label, stream_id):
        super(StreamNode, self).__init__(id, label)

        self.stream_id = stream_id

    def _serialise(self):
        return dict(super(StreamNode, self)._serialise(), streamid=self.stream_id)

    @staticmethod
    def _deserialise(json):
        return StreamNode(json.get("id"), json.get('label'), json['streamid'])

class MultiStreamNode(_GraphNode):
    def __init__(self, id, label, stream_ids):
        super(MultiStreamNode, self).__init__(id, label)

        self.stream_ids = stream_ids

    def _serialise(self):
        return dict(super(MultiStreamNode, self)._serialise(), streamids=self.stream_ids)

    @staticmethod
    def _deserialise(json):
        return MultiStreamNode(json.get("id"), json.get('label'), json['streamids'])

class GridNode(_GraphNode):
    def __init__(self, id, label, dataset, catalog=None):
        super(GridNode, self).__init__(id, label)

        self.catalog = catalog
        self.dataset = dataset

    def _serialise(self):
        result = super(GridNode, self)._serialise()
        result['dataset'] = self.dataset
        if self.catalog is not None:
            result['catalog'] = self.catalog
        return result

    @staticmethod
    def _deserialise(json):
        return GridNode(json.get("id"), json.get('label'), json['dataset'], json.get('catalog'))

class CollectionNode(_GraphNode):
    def __init__(self, id, label, collection):
        super(CollectionNode, self).__init__(id, label)

        self.collection = collection

    def _serialise(self):
        return dict(super(CollectionNode, self)._serialise(), collection=self.collection)

    @staticmethod
    def _deserialise(json):
        return CollectionNode(json["id"], json.get("label"), [_GraphNode._deserialise(v, True) for v in json.get("_embedded", {}).get("collection")])


class GraphConnection(object):
    def __init__(self, source_node, target_node, source_port=None, target_port=None):
        self._source_node = source_node
        self._target_node = target_node

        if isinstance(source_node, ModelNode) == (source_port is None):
            raise ValueError('The source_port must be specified if (and only if) source_node is a ModelNode.')
        if isinstance(target_node, ModelNode) == (target_port is None):
            raise ValueError('The target_port must be specified if (and only if) target_node is a ModelNode.')

        self._source_port = source_port
        self._target_port = target_port

    def _serialise(self):
        source = { 'node': self._source_node.id }
        if self._source_port is not None:
            source['port'] = self._source_port

        target = { 'node': self._target_node.id }
        if self._target_port is not None:
            target['port'] = self._target_port

        return { 'source': source, 'target': target }

class Port(object):
    """
    Representation of the "port" type used in the "model" type's "ports"
    property.

    Attributes:
        name: The port's name.
        required: True if a value must be supplied for the port when executing the model, otherwise False.
        type: The port's type (e.g. "stream", "multistream" or "document").
        description: A description of the port.
        direction: The port's direction (i.e. either "input" or "output").
    """
    def __init__(self, json):
        self.name = json.get('portname')
        self.required = json.get('required', False)
        self.type = json.get('type')
        self.description = json.get('description')
        self.direction = json.get('direction')

    def _serialise(self):
        return {
            'portname': self.name,
            'required': self.required,
            'type': self.type,
            'description': self.description,
            'direction': self.direction
        }

class WorkflowStatistics(object):
    """
    Representation of the execution statistics generated by running a workflow.

    Attributes:
        start_time: The time at which the workflow execution started, as an ISO-8601 timestamp.
        end_time: The time at which the workflow execution ended, as an ISO-8601 timestamp.
        status: The final status of the workflow.
        elapsed_time: The total time elapsed, as an ISO-8601 duration.
        errors: A list of error messages (if any were generated).
        log: A list of LogEntry instances, representing data captured by the internal logging frameworks.
        output: A list of OutputEntry instances, representing the console output generated by the executed model (if any).
    """
    def __init__(self, results, json):
        self._results = results

        self.start_time = json.get('starttime')
        self.end_time = json.get('endtime')
        self.status = json.get('status')
        self.elapsed_time = json.get('elapsedtime')
        self.errors = json.get('errors', [])
        self.log = [LogEntry(l) for l in json.get('log', [])]
        self.output = [OutputEntry(self, l) for l in json.get('output', [])]

    def _serialise(self):
        return {
            'startTime': self.start_time,
            'endTime': self.end_time,
            'status': self.status,
            'elapsedTime': self.elapsed_time,
            'errors': self.errors,
            'log': [entry._serialise() for entry in self.log],
            'output': [entry._serialise() for entry in self.output]
        }

class LogEntry(object):
    """
    Represents a log entry, as captured by the model execution framework's
    execution logging system.

    For Python-based models, these correspond to messages captured by the
    standard Python logging framework.

    Attributes:
        message: The log message.
        timestamp: The time at which the log message was generated, as an IS0-8601 timestamp.
        level: The message's severity level (i.e. "DEBUG", "INFO", "WARNING", "ERROR" or "CRITICAL").
        file: The filename of the file that generated the message (if known).
        line: The line number at which the message was generated (if known).
        logger: The ID of the logger which generated the message (if known).
    """
    def __init__(self, json):
        self.message = json.get('message')
        self.timestamp = json.get('timestamp')
        self.level = json.get('level')
        self.file = json.get('file')
        self.line = json.get('line')
        self.logger = json.get('logger')

    def _serialise(self):
        return {
            'message': self.message,
            'timestamp': self.timestamp,
            'level': self.level,
            'file': self.file,
            'line': self.line,
            'logger': self.logger
        }

class OutputEntry(object):
    """
    Represents an instance of console output from an executed model.

    Attributes:
        stream: The stream on which output occurred (i.e. "STDOUT" or "STDERR").
        content: The text that was output.
    """
    def __init__(self, statistics, json):
        self._statistics = statistics

        self.stream = json.get('stream')
        self.content = json.get('content')

    def _serialise(self):
        return {
            'stream': self.stream,
            'content': self.content
        }

class JobHistory(object):
    def __init__(self, json):
        self.status = json.get('status')
        self.timestamp = json.get('timestamp')

class JobResults(object):
    def __init__(self, json):
        stats = json.get('statistics', {})

        self.status = stats.get('status')
        self.start_time = stats.get('starttime')
        self.end_time = stats.get('endtime')
        self.elapsed_time = stats.get('elapsedtime')
        self.log = [LogEntry(e) for e in stats.get('log', [])]
        self.errors = stats.get('errors', [])

class RunAs(object):
    def __init__(self, json):
        self.roles = set(json.get('roles', []))

    def _serialise(self):
        return { 'roles': list(self.roles) }

    def clone(self):
        return RunAs({'roles': self.roles})

################################################################################
# Resource classes.                                                            #
################################################################################

class BaseImage(_Resource):
    """
    Representation of the API's "base image" resource type.

    Attributes:
        id: The base image's unique ID.
        name: The base image's name.
        description: A description of the base image.
        runtime_type: The model runtime types (i.e. "PYTHON" or "R") supported by the base image.
        model_root: The directory within the model's disk image into which the model's code and data files will be installed.
        model_user: The user under which the model runs.
        entrypoint_template: A template for the command that is executed in order to run the model.
        supported_providers: A set describing which package providers ("APT", "PIP" or "R_INSTALL") are supported by the base image.
        host_environment: An instance of HostEnvironment describing the host environment in which the base image is intended to be run.
        tags: A set of free-text "tags" relevant to the base image.
    """
    _url_path = 'base-images'
    _collection = 'baseImages'

    id = _IdProperty('id')
    name = _Property('name')
    description = _Property('description')
    runtime_type = _Property('runtimetype')
    model_root = _Property('modelroot')
    model_user = _Property('modeluser')
    entrypoint_template = _Property('entrypointtemplate')
    supported_providers = _Property('supportedproviders', set, list, set())
    host_environment = _Property('hostenvironment', lambda v: HostEnvironment(v), lambda v: v._serialise())
    tags = _Property('tags', set, list, set())


class Document(_Resource):
    """
    Representation of the API's "document node" resource type.

    If the `value_truncated` property is `False`, then the `value` property contains the document's value in full. If
    `value_truncated` is True, then the `value` property contains only an initial segment of the document value, and the
    full value of the document may be obtained using the client's `get_document_value` method.

    Attributes:
        id: the document node's unique ID.
        value_truncated: True if the value returned by the value property has been truncated, otherwise false
        organisation_id: The ID of the organisation that "owns" the document node.
        group_ids: A set containing the IDs of the group(s) that contain the document node (if any).
        value: The document node's value (possibly truncated).
    """
    _url_path = 'documentnodes'
    _collection = 'documentnodes'

    id = _IdProperty('documentid', writable=True, serialise_as='id')
    value_truncated = _Property('valuetruncated', serialize=False)
    organisation_id = _Property('organisationid', writable=True)
    group_ids = _Property('groupids', set, list, set(), writable=True)
    value = _Property('value', writable=True)


class Model(_Resource):
    """
    Representation of the API's "model" resource type.

    Attributes:
        id: The model's unique ID.
        name: The model's name.
        version: The model's version number.
        description: A description of what the model does.
        method: A description of how the model works.
        organisation_id: The ID of the organisation that "owns" the model.
        group_ids: A set containing the IDs of the group(s) that contain the model (if any).
        ports: A list of Port instances describing the model's ports.
    """
    _url_path = 'models'
    _collection = 'models'

    id = _IdProperty('id')
    name = _Property('name')
    version = _Property('version')
    description = _Property('description')
    method = _Property('method')
    organisation_id = _Property('organisationid')
    group_ids = _Property('groupids', set, list, set())
    ports = _EmbeddedProperty('ports', lambda v: [Port(p) for p in v], lambda v: [p._serialise() for p in v], [])

    def new_workflow(self):
        """
        Create a new instance of the Workflow class, representing a new workflow
        for running this model.

        The workflow that is created has the correct model ID assigned, has a
        placeholder name and description, has the same organisation ID and group
        IDs, and has placeholder values assigned to the ports attribute.

        Returns:
            A new instance of the Workflow class.
        """
        result = Workflow()
        result._client = self._client

        result.name = '{} Workflow'.format(self.name)
        result.description = '{} workflow'.format(self.name)
        result.model_id = self.id
        result.organisation_id = self.organisation_id
        result.group_ids = set(self.group_ids)
        result.ports = [WorkflowPort._deserialise(p._serialise()) for p in self.ports]

        return result

class Workflow(_Resource):
    """
    Representation of the API's "workflow" resource type.

    Attributes:
        id: The workflow's unique ID.
        name: The workflow's name.
        description: A description of the workflow.
        organisation_id: The ID of the organisation that "owns" the workflow.
        group_ids: A set containing the IDs of the group(s) that contain the workflow (if any).
        graph: The workflow's operator and data node graph.
    """
    _url_path = 'workflows'
    _collection = 'workflows'

    id = _IdProperty('id')
    name = _Property('name', writable=True)
    description = _Property('description', writable=True)
    organisation_id = _Property('organisationid', writable=True)
    group_ids = _Property('groupids', set, list, set(), writable=True)
    graph = _EmbeddedProperty('graph', lambda v: Graph(v), lambda v: v._serialise(), Graph())
    run_as = _EmbeddedProperty('runas', lambda v: RunAs(v), lambda v: v._serialise(), RunAs({'roles': []}))
    logs_truncated_to = _Property('logsTruncatedTo')

    def save(self, client=None):
        """
        Saves the workflow using the Client class' "post_workflow()" method.

        If the "client" argument is supplied, it must be an instance of the
        Client class, which is then used to save the workflow. Otherwise, the
        workflow must have been previously obtained with a call one of the
        Client class' workflow methods, in which case the workflow is saved
        using that same Client instance.

        Args:
            client: The Client class instance to use to save the workflow.

        Returns:
            The same Worfklow instance, updated with any new properties
            generated by the analysis service.

        Raises:
            ValueError: If the client parameter is omitted, and the Workflow
                instance hasn't been obtained through a previous call to one of
                the Client class' workflow methods.
            RequestError: If an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: If an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        if client is not None:
            self._client = client

        if self._client is None:
            raise ValueError('Cannot run workflow: no associated client.')

        return self._client.upload_workflow(self)

    def clone(self):
        """
        Generate a "clone" of the workflow.

        The returned workflow is functionally identical to the one this method
        is called on, with the exception that the ID is not set (since IDs are
        only assigned when the workflow is actually saved).

        Returns:
            A new Workflow instance, representing a clone of the instance that
            the method was called on.
        """
        result = Workflow()
        result._client = self._client

        result.name = self.name
        result.description = self.description
        result.organisation_id = self.organisation_id
        result.group_ids = set(self.group_ids)
        result.graph = self.graph.clone()
        result.run_as = self.run_as.clone()

        return result

    def run(self, debug=False, client=None):
        """
        Runs the workflow, using the Client class' "run_workflow()" method.

        If the "client" argument is supplied, it must be an instance of the
        Client class, which is then used to run the workflow. Otherwise, the
        workflow must have been previously obtained with a call one of the
        Client class' workflow methods, in which case the workflow is run
        using that same Client instance.

        Args:
            debug: If true, the workflow is run in "debug" mode (which causes
                additional log messages and output data to be returned in the
                response).
            client: The Client class instance to use to save the workflow.

        Returns:
            An instance of WorkflowResult representing the results of executing
            the workflow.

        Raises:
            ValueError: If the client parameter is omitted, and the Workflow
                instance hasn't been obtained through a previous call to one of
                the Client class' workflow methods.
            RequestError: If an HTTP "client error" (4XX) status code is
                returned by the server.
            ServerError: If an HTTP "server error" (5XX) status code is returned
                by the server.
        """
        if client is not None:
            self._client = client

        if self._client is None:
            raise ValueError('Cannot run workflow: no associated client.')

        return self._client.run_workflow(self, debug)

class Job(_Resource):
    _url_path = 'jobs'
    _collection = 'jobs'

    id = _IdProperty('id', serialize=False)
    workflow_id = _Property('workflowid', writable=True)
    debug = _Property('debug', writable=True, default=False)
    organisation_id = _Property('organisationid', serialize=False)
    group_ids = _Property('groupids', set, list, set(), serialize=False)
    schedule_id = _Property('scheduleid', serialize=False)
    status = _Property('status', serialize=False)
    timestamp = _Property('timestamp', serialize=False)
    history = _Property('history', lambda v: [JobHistory(h) for h in v], serialize=False)
    results = _EmbeddedProperty('results', serialize=False)

    def __init__(self, workflow_id=None, debug=False):
        self.workflow_id = workflow_id
        self.debug = debug

################################################################################
# Pseudo-resource classes.                                                     #
################################################################################

# NOTE: like the "real" resource classes, these classes represent the top-level
# response object returned by one (or more) of the API's endpoints. Where they
# differ from the real resource classes is that these represent transient
# entities.

class ModelInstallationResult(_Resource):
    """
    Represents the response from the API when installing a new model.

    Attributes:
        image_size: The size of the generated Docker image, in bytes.
        models: A list of Model instances describing the newly installed model(s) (there may be more than one).
    """
    def __init__(self, client, json):
        self.image_size = json.get('imagesize')
        self.models = [Model()._update(client, m) for m in json.get('_embedded', {}).get('models', [])]

class WorkflowResults(object):
    """
    Represents the response from the API when running a workflow.

    Attributes:
        id: The unique ID of the results.
        workflow_id: The ID of the workflow that generated the results.
        statistics: An instance of WorkflowStatistics describing the workflow's execution statistics (e.g. result status, run time, etc).
        ports: A List of WorkflowPort instances describing the new state of the workflow's ports.
    """
    def __init__(self, client, json):
        self._client = client

        self.id = json.get('id')
        self.workflow_id = json.get('workflowid')

        embedded = json.get('_embedded', {})
        self.statistics = WorkflowStatistics(self, embedded.get('statistics', {}))
        self.ports = [WorkflowPort._deserialise(p) for p in embedded.get('ports', {})]

    def _serialise(self):
        return {
            'id': self.id,
            'workflowId': self.workflow_id,
            'statistics': self.statistics._serialise(),
            'ports': [p._serialise() for p in self.ports]
        }
