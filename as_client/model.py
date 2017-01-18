
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
    def __init__(self, json_name, from_json=lambda v:v, default=None, writable=False):
        self.json_name = json_name
        self.from_json = from_json
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
        # TODO: check writable flag
        
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
    
    Does very little, except for provide an algorithm for updating a resource's
    property values when given a JSON object describing the resource.
    """
    def _update(self, client, json):
        self._client = client
        
        for prop_id, prop in inspect.getmembers(self.__class__, lambda m: isinstance(m, _Property)):
            prop._update(self, json)
        
        return self

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
    supported_providers = _Property('supportedproviders', set, set())
    host_environment = _Property('hostenvironment', lambda v: HostEnvironment(v))
    tags = _Property('tags', set, set())

class HostEnvironment(object):
    """
    Representation of the "host environment" type used in the "base image"
    type's "hostenvironment" property.
    """
    def __init__(self, json):
        self.architecture = json.get('architecture', None)
        self.operating_system = json.get('operatingsystem', None)

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
    group_ids = _Property('groupids', set, set())
    ports = _EmbeddedProperty('ports', lambda v: [Port(p) for p in v], [])

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
    group_ids = _Property('groupids', set, set())
    ports = _EmbeddedProperty('ports', lambda v: [WorkflowPort(p) for p in v], [])

class WorkflowPort(object):
    """
    Representation of the "port" type used in the "workflow" type's "ports"
    property.
    """
    def __init__(self, json):
        self.name = json.get('portname', None)
        self.required = json.get('required', False)
        self.type = json.get('type', None)
        self.description = json.get('description', None)
        self.direction = json.get('direction', None)
        
        # TODO: handle embedded datanodes.
