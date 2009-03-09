#!/usr/bin/env python
#
# Copyright 2007 Google Inc., 2008-2009 10gen Inc.
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

import logging
import sys
import threading
import types
import re
import random

from google.appengine.api import apiproxy_stub
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import datastore_index
from google.appengine.runtime import apiproxy_errors
from google.appengine.datastore import entity_pb

import pymongo
from pymongo.connection import Connection
from pymongo.binary import Binary

datastore_pb.Query.__hash__ = lambda self: hash(self.Encode())

_MAXIMUM_RESULTS = 1000
_MAX_QUERY_OFFSET = 1000
_MAX_QUERY_COMPONENTS = 100

class DatastoreMongoStub(apiproxy_stub.APIProxyStub):
  """Persistent stub for the Python datastore API, using MongoDB to persist.

  A DatastoreMongoStub instance handles a single app's data.
  """

  def __init__(self,
               app_id,
               datastore_file,
               history_file,
               require_indexes=False,
               service_name='datastore_v3'):
    """Constructor.

    Initializes the datastore stub.

    Args:
      app_id: string
      datastore_file: ignored
      history_file: ignored
      require_indexes: bool, default False.  If True, composite indexes must
          exist in index.yaml for queries that need them.
      service_name: Service name expected for all calls.
    """
    super(DatastoreMongoStub, self).__init__(service_name)


    assert isinstance(app_id, basestring) and app_id != ''
    self.__app_id = app_id
    self.__require_indexes = require_indexes

    # TODO should be a way to configure the connection
    self.__db = Connection()[app_id]

    # NOTE our query history gets reset each time the server restarts...
    # should this be fixed?
    self.__query_history = {}

    self.__next_index_id = 1
    self.__indexes = {}
    self.__index_lock = threading.Lock()

    self.__cursor_lock = threading.Lock()
    self.__next_cursor = 1
    self.__queries = {}

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
    if isinstance(value, datastore_types.ByteString):
      return {
        'class': 'bytes',
        'value': Binary(value)
        }
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
      if mongo_value['class'] == 'bytes':
        return datastore_types.ByteString(mongo_value['value'])
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
    raise apiproxy_errors.ApplicationError(
      datastore_pb.Error.BAD_REQUEST, "Can't handle operation %r." % operation)

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

    query_result.mutable_cursor().set_cursor(0)
    query_result.set_more_results(False)

    if self.__require_indexes:
      required, kind, ancestor, props, num_eq_filters = datastore_index.CompositeIndexForQuery(query)
      if required:
        index = entity_pb.CompositeIndex()
        index.mutable_definition().set_entity_type(kind)
        index.mutable_definition().set_ancestor(ancestor)
        for (k, v) in props:
          p = index.mutable_definition().add_property()
          p.set_name(k)
          p.set_direction(v)

        if props and not self.__has_index(index):
          raise apiproxy_errors.ApplicationError(
              datastore_pb.Error.NEED_INDEX,
              "This query requires a composite index that is not defined. "
              "You must update the index.yaml file in your application root.")

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

      if key in spec:
        if not isinstance(spec[key], types.DictType) and not isinstance(value, types.DictType):
          if spec[key] != value:
            return
        elif not isinstance(spec[key], types.DictType):
          value["$in"] = [spec[key]]
          spec[key] = value
        elif not isinstance(value, types.DictType):
          spec[key]["$in"] = [value]
        else:
          spec[key].update(value)
      else:
        spec[key] = value

    cursor = self.__db[collection].find(spec)

    order = self.__translate_order_for_mongo(query.order_list(), prototype)
    if order is None:
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
    query_result.set_more_results(True)

  def _Dynamic_Next(self, next_request, query_result):
    cursor = next_request.cursor().cursor()
    query_result.set_more_results(False)

    if cursor == 0: # we exited early from the query w/ no results...
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
    # TODO this is used for the admin viewer to introspect.
    pass

  def __collection_and_spec_for_index(self, index):
    def translate_name(ae_name):
      if ae_name == "__key__":
        return "_id"
      return ae_name

    def translate_direction(ae_dir):
      if ae_dir == 1:
        return pymongo.ASCENDING
      elif ae_dir == 2:
        return pymongo.DESCENDING
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'Weird direction.')

    collection = index.definition().entity_type()
    spec = []
    for prop in index.definition().property_list():
      spec.append((translate_name(prop.name()), translate_direction(prop.direction())))

    return (collection, spec)

  def __has_index(self, index):
    (collection, spec) = self.__collection_and_spec_for_index(index)
    if self.__db[collection]._gen_index_name(spec) in self.__db[collection].index_information().keys():
      return True
    return False

  def _Dynamic_CreateIndex(self, index, id_response):
    if index.id() != 0:
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'New index id must be 0.')
    elif self.__has_index(index):
      logging.getLogger().info(index)
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'Index already exists.')

    (collection, spec) = self.__collection_and_spec_for_index(index)

    if spec: # otherwise it's probably an index w/ just an ancestor specifier
      self.__db[collection].create_index(spec)
      if self.__db.error():
        raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                               "Error creating index. Maybe too many indexes?")

    # NOTE just give it a dummy id. we don't use these for anything...
    id_response.set_value(1)

  def _Dynamic_GetIndices(self, app_str, composite_indices):
    if app_str.value() != self.__db.name():
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             "Getting indexes for a different app unsupported.")

    def from_index_name(name):
      elements = name.split("_")
      index = []
      while len(elements):
        if not elements[0]:
          elements = elements[1:]
          elements[0] = "_" + elements[0]
        index.append((elements[0], int(elements[1])))
        elements = elements[2:]
      return index

    for collection in self.__db.collection_names():
      info = self.__db[collection].index_information()
      for index in info.keys():
        index_pb = entity_pb.CompositeIndex()
        index_pb.set_app_id(self.__db.name())
        index_pb.mutable_definition().set_entity_type(collection)
        index_pb.mutable_definition().set_ancestor(False)
        index_pb.set_state(2) # READ_WRITE
        index_pb.set_id(1) # bogus id
        for (k, v) in from_index_name(index):
          if k == "_id":
            k = "__key__"
          p = index_pb.mutable_definition().add_property()
          p.set_name(k)
          p.set_direction(v == pymongo.ASCENDING and 1 or 2)
        composite_indices.index_list().append(index_pb)

  def _Dynamic_UpdateIndex(self, index, void):
    logging.log(logging.WARN, 'update index unsupported')

  def _Dynamic_DeleteIndex(self, index, void):
    (collection, spec) = self.__collection_and_spec_for_index(index)
    if not spec:
      return

    if not self.__has_index(index):
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             "Index doesn't exist.")
    self.__db[collection].drop_index(spec)
