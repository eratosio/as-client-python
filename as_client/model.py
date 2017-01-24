
from __future__ import division

import collections, inspect

_UNKNOWN = object() # Placeholder for indicating an unknown value (since None might be a valid value).

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
    def __init__(self, json_name, from_json=lambda v:v, to_json=lambda v:v, default=None, writable=False):
        self.json_name = json_name
        self.from_json = from_json
        self.to_json = to_json
        self.default = default
        self.writable = writable
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        
        data = self.get_data(instance)
        
        if data.needs_fetch:
            id_ = getattr(instance, 'id', None)
            client = getattr(instance, '_client', None)
            if None not in (id, client):
                client._fetch_resource(owner, id_, instance)
        
        return data.local_value
    
    def __set__(self, instance, value):
        if not self.writable:
            cls = instance.__class__
            descriptors = inspect.getmembers(cls, lambda m: m is self)
            assert len(descriptors) == 1
            
            raise AttributeError('Property "{}" of class "{}" is not writable.'.format(descriptors[0][0], cls.__name__))
        
        self.get_data(instance).local_value = value
    
    def __del__(self, instance):
        pass # TODO
    
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
        local_value = self.get_data(instance).local_value
        if local_value is not _UNKNOWN:
            json[self.json_name] = self.to_json(local_value)

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
    
    This class does very little, other than providing an algorithm for updating
    a resource's property values when given a JSON object describing the
    resource.
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

class _ResourceList(collections.Sequence):
    """
    A thin wrapper around a list of resource class instances.
    
    Given a JSON object representing a HAL-encapsulated list of resources,
    deserialises the resource instances into a list and extracts the associated
    paging metadata.
    """
    def __init__(self, client, type_, json):
        self._type = type_
        
        self._items = [type_()._update(client, v) for v in json.get('_embedded', {}).get(type_._collection, [])]
        self._skip = json.get('skip', None)
        self._limit = json.get('limit', None)
        self._count = json.get('count', None)
        self._total_count = json.get('totalcount', None)
    
    def __getitem__(self, index):
        return self._items[index]
    
    def __len__(self):
        return len(self._items)
    
    skip = property(lambda self: self._skip)
    limit = property(lambda self: self._limit)
    count = property(lambda self: self._count)
    total_count = property(lambda self: self._total_count)

class _ResourceCollection(collections.Sequence):
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
# Resource classes.                                                            #
################################################################################

class BaseImage(_Resource):
    """
    Representation of the API's "base image" resource type.
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

class Model(_Resource):
    """
    Representation of the API's "model" resource type.
    """
    _url_path = 'models'
    _collection = 'models'
    
    id = _IdProperty('id')
    name = _Property('name')
    version = _Property('version')
    description = _Property('description')
    organisation_id = _Property('organisationid')
    group_ids = _Property('groupids', set, list, set())
    ports = _EmbeddedProperty('ports', lambda v: [Port(p) for p in v], lambda v: [p._serialise() for p in v], [])

class Workflow(_Resource):
    """
    Representation of the API's "workflow" resource type.
    """
    _url_path = 'workflows'
    _collection = 'workflows'
    
    id = _IdProperty('id')
    name = _Property('name')
    description = _Property('description')
    model_id = _Property('modelid')
    organisation_id = _Property('organisationid')
    group_ids = _Property('groupids', set, list, set())
    ports = _EmbeddedProperty('ports', lambda v: [WorkflowPort._deserialise(p) for p in v], lambda v: [p._serialise() for p in v], [])
    
    def run(self, debug=False, client=None):
        if client is not None:
            self._client = client
        
        if self._client is None:
            raise ValueError('Cannot run workflow: no associated client.')
        
        return self._client.run_workflow(self, debug)

################################################################################
# Pseudo-resource classes.                                                     #
################################################################################

# NOTE: like the "real" resource classes, these classes represent the top-level
# response object returned by one (or more) of the API's endpoints. Where they
# differ from the real resource classes is that these represent transient
# entities. 

class ModelInstallationResult(_Resource):
    """
    Representation of the response of a POST request to the API's /models
    endpoint.
    """
    def __init__(self, client, json):
        self.image_size = json.get('imagesize', None)
        self.models = [Model()._update(client, m) for m in json.get('_embedded', {}).get('models', [])]

class WorkflowResults(object):
    def __init__(self, client, json):
        self._client = client
        
        self.id = json.get('id', None)
        self.workflow_id = json.get('workflowid', None)
        
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

################################################################################
# Resource component classes.                                                  #
################################################################################

class HostEnvironment(object):
    """
    Representation of the "host environment" type used in the "base image"
    type's "hostenvironment" property.
    """
    def __init__(self, json):
        self.architecture = json.get('architecture', None)
        self.operating_system = json.get('operatingsystem', None)
    
    def _serialise(self):
        return {
            'architecture': self.architecture,
            'operatingsystem': self.operating_system
        }

class Port(object):
    """
    Representation of the "port" type used in the "model" type's "ports"
    property.
    """
    def __init__(self, json):
        self.name = json.get('portname', None)
        self.required = json.get('required', False)
        self.type = json.get('type', None)
        self.description = json.get('description', None)
        self.direction = json.get('direction', None)
    
    def _serialise(self):
        return {
            'portname': self.name,
            'required': self.required,
            'type': self.type,
            'description': self.description,
            'direction': self.direction
        }

class DataNode(object):
    def __init__(self, port, json):
        self._port = port
        
        self.id = json.get('id', None)
        self.type = json.get('type', None)
        self.organisation_id = json.get('organisationid', None)
        self.group_ids = set(json.get('groupids', []))

class DocumentNode(DataNode):
    def __init__(self, port, json):
        super(DocumentNode, self).__init__(port, json)
        
        self.value = json.get('value', None)
    
    def _serialise(self):
        return { 'value': self.value } if self.value else { 'id': self.id }

class StreamNode(DataNode):
    def __init__(self, port, json):
        super(StreamNode, self).__init__(port, json)
        
        self.stream_id = json.get('streamid', None)
    
    def _serialise(self):
        return { 'streamid': self.stream_id } if self.stream_id else { 'id': self.id }

class MultistreamNode(DataNode):
    def __init__(self, port, json):
        super(MultistreamNode, self).__init__(port, json)
        
        self.stream_ids = set(json.get('streamids', []))
    
    def _serialise(self):
        return { 'streamids': list(self.stream_ids) } if self.stream_ids else { 'id': self.id }

class WorkflowPort(object):
    """
    Representation of the "port" type used in the "workflow" type's "ports"
    property.
    """
    def _serialise(self):
        return {
            'portname': self.name,
            'datanode': self.datanode._serialise()
        }
    
    @staticmethod
    def _deserialise(json):
        try:
            typename = json['type']
        except KeyError:
            raise ValueError('Invalid workflow port: "type" not specified.')
        
        matching_types = [p for p in WorkflowPort.__subclasses__() if p._type == typename.lower()]
        if not matching_types:
            raise ValueError('Unknown workflow port type "{}".'.format(typename))
        
        assert len(matching_types) == 1
        type_ = matching_types[0]
        
        result = type_()
        result.name = json.get('portname', None)
        result.required = json.get('required', False)
        result.type = json.get('type', None)
        result.description = json.get('description', None)
        result.direction = json.get('direction', None)
        result.datanode = type_._datanode_type(result, json.get('_embedded', {}).get('datanode', {}))
        
        return result

class DocumentWorkflowPort(WorkflowPort):
    _type = 'document'
    _datanode_type = DocumentNode
    
    @property
    def value(self):
        return self.datanode.value
    
    @value.setter
    def value(self, new_value):
        self.datanode.value = new_value

class StreamWorkflowPort(WorkflowPort):
    _type = 'stream'
    _datanode_type = StreamNode
    
    @property
    def stream_id(self):
        return self.datanode.stream_id
    
    @stream_id.setter
    def stream_id(self, new_stream_id):
        self.datanode.stream_id = new_stream_id

class MultistreamWorkflowPort(WorkflowPort):
    _type = 'multistream'
    _datanode_type = MultistreamNode
    
    @property
    def stream_ids(self):
        return self.datanode.stream_ids
    
    @stream_ids.setter
    def stream_ids(self, new_stream_ids):
        self.datanode.stream_ids = new_stream_ids

class WorkflowStatistics(object):
    def __init__(self, results, json):
        self._results = results
        
        self.start_time = json.get('starttime', None)
        self.end_time = json.get('endtime', None)
        self.status = json.get('status', None)
        self.elapsed_time = json.get('elapsedtime', None)
        self.errors = json.get('errors', [])
        self.log = [LogEntry(self, l) for l in json.get('log', [])]
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
    def __init__(self, statistics, json):
        self._statistics = statistics
        
        self.message = json.get('message', None)
        self.timestamp = json.get('timestamp', None)
        self.level = json.get('level', None)
        self.file = json.get('file', None)
        self.line = json.get('line', None)
        self.logger = json.get('logger', None)
    
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
    def __init__(self, statistics, json):
        self._statistics = statistics
        
        self.stream = json.get('stream', None)
        self.content = json.get('content', None)
    
    def _serialise(self):
        return {
            'stream': self.stream,
            'content': self.content
        }
