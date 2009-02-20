#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
MongoDB backed stub for the Python datastore API.

Transactions are unsupported.
"""

import datetime
import logging
import os
import struct
import sys
import tempfile
import threading
import warnings
import types
import re
import random

from google.appengine.api import api_base_pb
from google.appengine.api import apiproxy_stub
from google.appengine.api import datastore
from google.appengine.api import datastore_admin
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import datastore_index
from google.appengine.runtime import apiproxy_errors
from google.net.proto import ProtocolBuffer
from google.appengine.datastore import entity_pb

import pymongo
from pymongo.connection import Connection
from pymongo.binary import Binary

warnings.filterwarnings('ignore', 'tempnam is a potential security risk')


entity_pb.Reference.__hash__ = lambda self: hash(self.Encode())
datastore_pb.Query.__hash__ = lambda self: hash(self.Encode())


_MAXIMUM_RESULTS = 1000


_MAX_QUERY_OFFSET = 1000


_MAX_QUERY_COMPONENTS = 100


class _StoredEntity(object):
  """Simple wrapper around an entity stored by the stub.

  Public properties:
    protobuf: Native protobuf Python object, entity_pb.EntityProto.
    encoded_protobuf: Encoded binary representation of above protobuf.
    native: datastore.Entity instance.
  """

  def __init__(self, entity):
    """Create a _StoredEntity object and store an entity.

    Args:
      entity: entity_pb.EntityProto to store.
    """
    self.protobuf = entity

    self.encoded_protobuf = entity.Encode()

    self.native = datastore.Entity._FromPb(entity)


class DatastoreMongoStub(apiproxy_stub.APIProxyStub):
  """ Persistent stub for the Python datastore API.

  Stores all entities in memory, and persists them to a file as pickled
  protocol buffers. A DatastoreMongoStub instance handles a single app's data
  and is backed by files on disk.
  """
  _PROPERTY_TYPE_TAGS = {
    datastore_types.Blob: entity_pb.PropertyValue.kstringValue,
    bool: entity_pb.PropertyValue.kbooleanValue,
    datastore_types.Category: entity_pb.PropertyValue.kstringValue,
    datetime.datetime: entity_pb.PropertyValue.kint64Value,
    datastore_types.Email: entity_pb.PropertyValue.kstringValue,
    float: entity_pb.PropertyValue.kdoubleValue,
    datastore_types.GeoPt: entity_pb.PropertyValue.kPointValueGroup,
    datastore_types.IM: entity_pb.PropertyValue.kstringValue,
    int: entity_pb.PropertyValue.kint64Value,
    datastore_types.Key: entity_pb.PropertyValue.kReferenceValueGroup,
    datastore_types.Link: entity_pb.PropertyValue.kstringValue,
    long: entity_pb.PropertyValue.kint64Value,
    datastore_types.PhoneNumber: entity_pb.PropertyValue.kstringValue,
    datastore_types.PostalAddress: entity_pb.PropertyValue.kstringValue,
    datastore_types.Rating: entity_pb.PropertyValue.kint64Value,
    str: entity_pb.PropertyValue.kstringValue,
    datastore_types.Text: entity_pb.PropertyValue.kstringValue,
    type(None): 0,
    unicode: entity_pb.PropertyValue.kstringValue,
    users.User: entity_pb.PropertyValue.kUserValueGroup,
    }

  WRITE_ONLY = entity_pb.CompositeIndex.WRITE_ONLY
  READ_WRITE = entity_pb.CompositeIndex.READ_WRITE
  DELETED = entity_pb.CompositeIndex.DELETED
  ERROR = entity_pb.CompositeIndex.ERROR

  _INDEX_STATE_TRANSITIONS = {
    WRITE_ONLY: frozenset((READ_WRITE, DELETED, ERROR)),
    READ_WRITE: frozenset((DELETED,)),
    ERROR: frozenset((DELETED,)),
    DELETED: frozenset((ERROR,)),
  }

  def __init__(self,
               app_id,
               datastore_file,
               history_file,
               require_indexes=False,
               service_name='datastore_v3'):
    """Constructor.

    Initializes and loads the datastore from the backing files, if they exist.

    Args:
      app_id: string
      datastore_file: string, stores all entities across sessions.  Use None
          not to use a file.
      history_file: string, stores query history.  Use None as with
          datastore_file.
      require_indexes: bool, default False.  If True, composite indexes must
          exist in index.yaml for queries that need them.
      service_name: Service name expected for all calls.
    """
    super(DatastoreMongoStub, self).__init__(service_name)


    assert isinstance(app_id, basestring) and app_id != ''
    self.__app_id = app_id
    self.__require_indexes = require_indexes
    self.__db = Connection()[app_id]

    # NOTE our query history gets reset each time the server restarts...
    # should this be fixed?
    self.__query_history = {}

    self.__index_id_lock = threading.Lock()
    self.__next_index_id = 1

    self.__cursor_lock = threading.Lock()
    self.__next_cursor = 1
    self.__queries = {}

    # TODO build indexes from index.yaml

  def MakeSyncCall(self, service, call, request, response):
    """ The main RPC entry point. service must be 'datastore_v3'. So far, the
    supported calls are 'Get', 'Put', 'RunQuery', 'Next', and 'Count'.
    """
    super(DatastoreMongoStub, self).MakeSyncCall(service,
                                                call,
                                                request,
                                                response)

    explanation = []
    assert response.IsInitialized(explanation), explanation

  def QueryHistory(self):
    """Returns a dict that maps Query PBs to times they've been run.
    """
    return dict((pb, times) for pb, times in self.__query_history.items()
                if pb.app() == self.__app_id)

  def __collection_for_key(self, key):
    return key.path().element(-1).type()

  def __id_for_key(self, key):
    db_path = []
    def add_element_to_db_path(elem):
      db_path.append(elem.type())
      if elem.has_name():
        db_path.append(elem.name())
      else:
        db_path.append("\t" + str(elem.id()))
    for elem in key.path().element_list():
      add_element_to_db_path(elem)
    return "\10".join(db_path)

  def __key_for_id(self, id):
    def from_db(value):
      if value.startswith("\t"):
        return int(value[1:])
      return value
    return datastore_types.Key.from_path(*[from_db(a) for a in id.split("\10")])

  def __create_mongo_value_for_value(self, value):
    if isinstance(value, datastore_types.Rating):
      return {
        'class': 'rating',
        'rating': int(value),
        }
    if isinstance(value, datastore_types.Category):
      return {
        'class': 'category',
        'category': str(value),
        }
    if isinstance(value, datastore_types.Key):
      return {
        'class': 'key',
        'path': self.__id_for_key(value._ToPb()),
        }
    if isinstance(value, types.ListType):
      list_for_db = [self.__create_mongo_value_for_value(v) for v in value]
      sorted_list = sorted(value)
      return {
        'class': 'list',
        'list': list_for_db,
        'ascending_sort_key': self.__create_mongo_value_for_value(sorted_list[0]),
        'descending_sort_key': self.__create_mongo_value_for_value(sorted_list[-1]),
        }
    if isinstance(value, users.User):
      return {
        'class': 'user',
        'email': value.email(),
        }
    if isinstance(value, datastore_types.Text):
      return {
        'class': 'text',
        'string': unicode(value),
        }
    if isinstance(value, datastore_types.Blob):
      return Binary(value)
    if isinstance(value, datastore_types.IM):
      return {
        'class': 'im',
        'protocol': value.protocol,
        'address': value.address,
        }
    if isinstance(value, datastore_types.GeoPt):
      return {
        'class': 'geopt',
        'lat': value.lat,
        'lon': value.lon,
        }
    if isinstance(value, datastore_types.Email):
      return {
        'class': 'email',
        'value': value,
        }
    return value

  def __create_value_for_mongo_value(self, mongo_value):
    if isinstance(mongo_value, Binary):
      return datastore_types.Blob(str(mongo_value))
    if isinstance(mongo_value, types.DictType):
      if mongo_value['class'] == 'rating':
        return datastore_types.Rating(int(mongo_value["rating"]))
      if mongo_value['class'] == 'category':
        return datastore_types.Category(mongo_value["category"])
      if mongo_value['class'] == 'key':
        return self.__key_for_id(mongo_value['path'])
      if mongo_value['class'] == 'list':
        return [self.__create_value_for_mongo_value(v) for v in mongo_value['list']]
      if mongo_value['class'] == 'user':
        return users.User(email=mongo_value["email"])
      if mongo_value['class'] == 'text':
        return datastore_types.Text(mongo_value['string'])
      if mongo_value['class'] == 'im':
        return datastore_types.IM(mongo_value['protocol'], mongo_value['address'])
      if mongo_value['class'] == 'geopt':
        return datastore_types.GeoPt(mongo_value['lat'], mongo_value['lon'])
      if mongo_value['class'] == 'email':
        return datastore_types.Email(mongo_value['value'])
    return mongo_value

  def __mongo_document_for_entity(self, entity):
    document = {}
    document["_id"] = self.__id_for_key(entity.key())

    entity = datastore.Entity._FromPb(entity)
    for (k, v) in entity.iteritems():
      v = self.__create_mongo_value_for_value(v)
      document[k] = v

    return document

  def __entity_for_mongo_document(self, document):
    key = self.__key_for_id(document.pop("_id"))
    entity = datastore.Entity(kind=key.kind(), parent=key.parent(), name=key.name())

    for k in document.keys():
      v = self.__create_value_for_mongo_value(document[k])
      entity[k] = v

    pb = entity._ToPb()
    # no decent way to initialize an Entity w/ an existing key...
    if not key.name():
      pb.key().path().element_list()[-1].set_id(key.id())

    return pb

  def _Dynamic_Put(self, put_request, put_response):
    for entity in put_request.entity_list():
      clone = entity_pb.EntityProto()
      clone.CopyFrom(entity)

      assert clone.has_key()
      assert clone.key().path().element_size() > 0

      last_path = clone.key().path().element_list()[-1]
      if last_path.id() == 0 and not last_path.has_name():
        # HACK just using a random id...
        last_path.set_id(random.randint(-sys.maxint-1, sys.maxint))

        assert clone.entity_group().element_size() == 0
        group = clone.mutable_entity_group()
        root = clone.key().path().element(0)
        group.add_element().CopyFrom(root)

      else:
        assert (clone.has_entity_group() and
                clone.entity_group().element_size() > 0)

      collection = self.__collection_for_key(clone.key())
      document = self.__mongo_document_for_entity(clone)

      id = self.__db[collection].save(document)
      put_response.key_list().append(self.__key_for_id(id)._ToPb())

  def _Dynamic_Get(self, get_request, get_response):
    for key in get_request.key_list():
        collection = self.__collection_for_key(key)
        id = self.__id_for_key(key)

        group = get_response.add_entity()
        document = self.__db[collection].find_one({"_id": id})
        if document is None:
          entity = None
        else:
          entity = self.__entity_for_mongo_document(document)

        if entity:
          group.mutable_entity().CopyFrom(entity)

  def _Dynamic_Delete(self, delete_request, delete_response):
    for key in delete_request.key_list():
      collection = self.__collection_for_key(key)
      id = self.__id_for_key(key)
      self.__db[collection].remove({"_id": id})

  def __special_props(self, value, direction):
    if isinstance(value, datastore_types.Category):
      return ["category"]
    if isinstance(value, datastore_types.GeoPt):
      return ["lat", "lon"]
    if isinstance(value, list):
      if direction == pymongo.ASCENDING:
        return ["ascending_sort_key"]
      return ["descending_sort_key"]
    return None

  def __unorderable(self, value):
    if isinstance(value, datastore_types.Text):
      return True
    if isinstance(value, datastore_types.Blob):
      return True
    return False

  def __translate_order_for_mongo(self, order_list, prototype):
    mongo_ordering = []

    for o in order_list:
      key = o.property().decode('utf-8')
      value = pymongo.ASCENDING
      if o.direction() is datastore_pb.Query_Order.DESCENDING:
        value = pymongo.DESCENDING

      if key == "__key__":
        key = "_id"
        mongo_ordering.append((key, value))
        continue

      if key not in prototype or self.__unorderable(prototype[key]):
        return None

      props = self.__special_props(prototype[key], value)
      if props:
        for prop in props:
          mongo_ordering.append((key + "." + prop, value))
      else:
        mongo_ordering.append((key, value))
    return mongo_ordering

  def __filter_suffix(self, value):
    if isinstance(value, types.ListType):
      return ".list"
    return ""

  def __filter_binding(self, key, value, operation, prototype):
    if key in prototype:
      key += self.__filter_suffix(prototype[key])

    if key == "__key__":
      key = "_id"
      value = self.__id_for_key(value._ToPb())
    else:
      value = self.__create_mongo_value_for_value(value)

    if operation == "<":
      return (key, {'$lt': value})
    elif operation == '<=':
      return (key, {'$lte': value})
    elif operation == '>':
      return (key, {'$gt': value})
    elif operation == '>=':
      return (key, {'$gte': value})
    elif operation == '==':
      return (key, value)
    raise datastore_errors.InternalError("operation %s doesn't work..." % operation)

  def _Dynamic_RunQuery(self, query, query_result):
    if query.has_offset() and query.offset() > _MAX_QUERY_OFFSET:
      raise apiproxy_errors.ApplicationError(
          datastore_pb.Error.BAD_REQUEST, 'Too big query offset.')

    num_components = len(query.filter_list()) + len(query.order_list())
    if query.has_ancestor():
      num_components += 1
    if num_components > _MAX_QUERY_COMPONENTS:
      raise apiproxy_errors.ApplicationError(
          datastore_pb.Error.BAD_REQUEST,
          ('query is too large. may not have more than %s filters'
           ' + sort orders ancestor total' % _MAX_QUERY_COMPONENTS))

    app = query.app()

# TODO do this check
#     if self.__require_indexes:
#       required, kind, ancestor, props, num_eq_filters = datastore_index.CompositeIndexForQuery(query)
#       if required:
#         required_key = kind, ancestor, props
#         indexes = self.__indexes.get(app)
#         if not indexes:
#           raise apiproxy_errors.ApplicationError(
#               datastore_pb.Error.NEED_INDEX,
#               "This query requires a composite index, but none are defined. "
#               "You must create an index.yaml file in your application root.")
#         eq_filters_set = set(props[:num_eq_filters])
#         remaining_filters = props[num_eq_filters:]
#         for index in indexes:
#           definition = datastore_admin.ProtoToIndexDefinition(index)
#           index_key = datastore_index.IndexToKey(definition)
#           if required_key == index_key:
#             break
#           if num_eq_filters > 1 and (kind, ancestor) == index_key[:2]:
#             this_props = index_key[2]
#             this_eq_filters_set = set(this_props[:num_eq_filters])
#             this_remaining_filters = this_props[num_eq_filters:]
#             if (eq_filters_set == this_eq_filters_set and
#                 remaining_filters == this_remaining_filters):
#               break
#         else:
#           raise apiproxy_errors.ApplicationError(
#               datastore_pb.Error.NEED_INDEX,
#               "This query requires a composite index that is not defined. "
#               "You must update the index.yaml file in your application root.")

    collection = query.kind()

    clone = datastore_pb.Query()
    clone.CopyFrom(query)
    clone.clear_hint()
    if clone in self.__query_history:
      self.__query_history[clone] += 1
    else:
      self.__query_history[clone] = 1

    # HACK we need to get one Entity from this collection so we know what the
    # property types are (because we need to construct queries that depend on
    # the types of the properties)...
    prototype = self.__db[collection].find_one()
    if prototype is None:
      query_result.mutable_cursor().set_cursor(0)
      query_result.set_more_results(False)
      return
    prototype = datastore.Entity._FromPb(self.__entity_for_mongo_document(prototype))

    spec = {}

    if query.has_ancestor():
      spec["_id"] = re.compile("^%s.*$" % self.__id_for_key(query.ancestor()))

    operators = {datastore_pb.Query_Filter.LESS_THAN:             '<',
                 datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL:    '<=',
                 datastore_pb.Query_Filter.GREATER_THAN:          '>',
                 datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL: '>=',
                 datastore_pb.Query_Filter.EQUAL:                 '==',
                 }

    for filt in query.filter_list():
      assert filt.op() != datastore_pb.Query_Filter.IN

      prop = filt.property(0).name().decode('utf-8')
      op = operators[filt.op()]

      filter_val_list = [datastore_types.FromPropertyPb(filter_prop)
                         for filter_prop in filt.property_list()]

      (key, value) = self.__filter_binding(prop, filter_val_list[0], op, prototype)

      # TODO this can't be the right way to handle this case... need more tests
      if key in spec:
        query_result.mutable_cursor().set_cursor(0)
        query_result.set_more_results(False)
        return

      spec[key] = value

#     logging.getLogger().info(spec)

    cursor = self.__db[collection].find(spec)

    order = self.__translate_order_for_mongo(query.order_list(), prototype)
    if order is None:
      query_result.mutable_cursor().set_cursor(0)
      query_result.set_more_results(False)
      return
    if order:
      cursor = cursor.sort(order)

    if query.has_offset():
      cursor = cursor.skip(query.offset())
    if query.has_limit():
      cursor = cursor.limit(query.limit())

    self.__cursor_lock.acquire()
    cursor_index = self.__next_cursor
    self.__next_cursor += 1
    self.__cursor_lock.release()
    self.__queries[cursor_index] = cursor

    query_result.mutable_cursor().set_cursor(cursor_index)
    query_result.set_more_results(True) # see if we can get away with always feigning more results

  def _Dynamic_Next(self, next_request, query_result):
    cursor = next_request.cursor().cursor()

    if cursor == 0: # we exited early from the query w/ no results...
      query_result.set_more_results(False)
      return

    try:
      cursor = self.__queries[cursor]
    except KeyError:
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'Cursor %d not found' % cursor)

    count = next_request.count()
    for _ in range(count):
      try:
        query_result.result_list().append(self.__entity_for_mongo_document(cursor.next()))
      except StopIteration:
        query_result.set_more_results(False)
        return
    query_result.set_more_results(True)

  def _Dynamic_Count(self, query, integer64proto):
    query_result = datastore_pb.QueryResult()
    self._Dynamic_RunQuery(query, query_result)
    cursor_number = query_result.cursor().cursor()
    if cursor_number == 0: # we exited early from the query w/ no results...
      integer64proto.set_value(0)
    else:
      cursor = self.__queries[cursor_number]
      count = cursor.count()
      del self.__queries[cursor_number]
      if query.has_limit() and count > query.limit():
        count = query.limit()
      integer64proto.set_value(count)

  def _Dynamic_BeginTransaction(self, request, transaction):
    transaction.set_handle(0)
    logging.log(logging.WARN, 'transactions unsupported')

  def _Dynamic_Commit(self, transaction, transaction_response):
    logging.log(logging.WARN, 'transactions unsupported')

  def _Dynamic_Rollback(self, transaction, transaction_response):
    logging.log(logging.WARN, 'transactions unsupported')

  def _Dynamic_GetSchema(self, app_str, schema):
    pass
#     minint = -sys.maxint - 1
#     try:
#       minfloat = float('-inf')
#     except ValueError:
#       minfloat = -1e300000

#     app_str = app_str.value()

#     kinds = []

#     for app, kind in self.__entities:
#       if app == app_str:
#         app_kind = (app, kind)
#         if app_kind in self.__schema_cache:
#           kinds.append(self.__schema_cache[app_kind])
#           continue

#         kind_pb = entity_pb.EntityProto()
#         kind_pb.mutable_key().set_app('')
#         kind_pb.mutable_key().mutable_path().add_element().set_type(kind)
#         kind_pb.mutable_entity_group()

#         props = {}

#         for entity in self.__entities[app_kind].values():
#           for prop in entity.protobuf.property_list():
#             if prop.name() not in props:
#               props[prop.name()] = entity_pb.PropertyValue()
#             props[prop.name()].MergeFrom(prop.value())

#         for value_pb in props.values():
#           if value_pb.has_int64value():
#             value_pb.set_int64value(minint)
#           if value_pb.has_booleanvalue():
#             value_pb.set_booleanvalue(False)
#           if value_pb.has_stringvalue():
#             value_pb.set_stringvalue('')
#           if value_pb.has_doublevalue():
#             value_pb.set_doublevalue(minfloat)
#           if value_pb.has_pointvalue():
#             value_pb.mutable_pointvalue().set_x(minfloat)
#             value_pb.mutable_pointvalue().set_y(minfloat)
#           if value_pb.has_uservalue():
#             value_pb.mutable_uservalue().set_gaiaid(minint)
#             value_pb.mutable_uservalue().set_email('')
#             value_pb.mutable_uservalue().set_auth_domain('')
#             value_pb.mutable_uservalue().clear_nickname()
#           elif value_pb.has_referencevalue():
#             value_pb.clear_referencevalue()
#             value_pb.mutable_referencevalue().set_app('')

#         for name, value_pb in props.items():
#           prop_pb = kind_pb.add_property()
#           prop_pb.set_name(name)
#           prop_pb.set_multiple(False)
#           prop_pb.mutable_value().CopyFrom(value_pb)

#         kinds.append(kind_pb)
#         self.__schema_cache[app_kind] = kind_pb

#     for kind_pb in kinds:
#       schema.add_kind().CopyFrom(kind_pb)

  def __FindIndex(self, index):
    """Finds an existing index by definition.

    Args:
      definition: entity_pb.CompositeIndex

    Returns:
      entity_pb.CompositeIndex, if it exists; otherwise None
    """
    app = index.app_id()
    if app in self.__indexes:
      for stored_index in self.__indexes[app]:
        if index.definition() == stored_index.definition():
          return stored_index

    return None

  def _Dynamic_CreateIndex(self, index, id_response):
    if index.id() != 0:
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'New index id must be 0.')
    elif self.__FindIndex(index):
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'Index already exists.')

    self.__index_id_lock.acquire()
    index.set_id(self.__next_index_id)
    id_response.set_value(self.__next_index_id)
    self.__next_index_id += 1
    self.__index_id_lock.release()

    clone = entity_pb.CompositeIndex()
    clone.CopyFrom(index)
    app = index.app_id()
    clone.set_app_id(app)

    self.__indexes_lock.acquire()
    try:
      if app not in self.__indexes:
        self.__indexes[app] = []
      self.__indexes[app].append(clone)
    finally:
      self.__indexes_lock.release()

  def _Dynamic_GetIndices(self, app_str, composite_indices):
    pass
#     composite_indices.index_list().extend(
#       self.__indexes.get(app_str.value(), []))

  def _Dynamic_UpdateIndex(self, index, void):
    pass
#     stored_index = self.__FindIndex(index)
#     if not stored_index:
#       raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
#                                              "Index doesn't exist.")
#     elif (index.state() != stored_index.state() and
#           index.state() not in self._INDEX_STATE_TRANSITIONS[stored_index.state()]):
#       raise apiproxy_errors.ApplicationError(
#         datastore_pb.Error.BAD_REQUEST,
#         "cannot move index state from %s to %s" %
#           (entity_pb.CompositeIndex.State_Name(stored_index.state()),
#           (entity_pb.CompositeIndex.State_Name(index.state()))))

#     self.__indexes_lock.acquire()
#     try:
#       stored_index.set_state(index.state())
#     finally:
#       self.__indexes_lock.release()

  def _Dynamic_DeleteIndex(self, index, void):
    pass
#     stored_index = self.__FindIndex(index)
#     if not stored_index:
#       raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
#                                              "Index doesn't exist.")

#     app = index.app_id()
#     self.__indexes_lock.acquire()
#     try:
#       self.__indexes[app].remove(stored_index)
#     finally:
#       self.__indexes_lock.release()

  @classmethod
  def __GetSpecialPropertyValue(cls, entity, property):
    """Returns an entity's value for a special property.

    Right now, the only special property is __key__, whose value is the
    entity's key.

    Args:
      entity: datastore.Entity

    Returns:
      property value. For __key__, a datastore_types.Key.

    Raises:
      AssertionError, if the given property is not special.
    """
    assert property in datastore_types._SPECIAL_PROPERTIES
    if property == datastore_types._KEY_SPECIAL_PROPERTY:
      return entity.key()

