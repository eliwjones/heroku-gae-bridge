from db import flatten, unflatten
from google.appengine.ext import ndb
from google.appengine.api import namespace_manager

class GaeDatastoreWrapper(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        if 'data_wrapper' not in app.extensions:
            app.extensions['data_wrapper'] = {}
            # Not really sure if care to have ns be ENV or DB_NAME or both..
            self._ns = app.config['ENV']

    @property
    def ns(self):
        return self._ns

    def create_class(self, name, key_name = None):
        db_class = type(name, (ndb.Expando,), {})
        if key_name:
            return db_class(id = key_name, namespace = self.ns)
        else:
            return db_class(namespace = self.ns)

    def build_query(self, table, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(table)
        query = ndb.Query(kind = collection.__class__.__name__, namespace = self.ns)
        results = None
        if key_name:
            return ndb.Key(collection.__class__.__name__, key_name, namespace = self.ns)
        flattened_props = flatten(properties)
        for prop in flattened_props:
            query.filter(ndb.GenericProperty(prop) == flattened_props[prop])
        return query

    def get(self, table, properties, key_only = False):
        query = self.build_query(table, properties)
        result = query.get()
        if result:
            if key_only:
                return result.key
            key_name = result.key.id()
            result = unflatten(result.to_dict())
            result['_id'] = key_name
        return result

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        ndb.delete_multi(keys_to_delete)

    def put(self, table, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(table, key_name)
        flattened_props = flatten(properties)
        for key in flattened_props:
            setattr(collection, key, flattened_props[key])
        return collection.put().id()

    def find(self, table, properties, limit = None, keys_only = False):
        if '_id' in properties:
            return [self.get(table, properties, key_only = keys_only)]
        query = self.build_query(table, properties)
        results = []
        cursor = query.iter(limit=limit, keys_only = keys_only)
        if keys_only:
            results = list(cursor)
        else:
            results = self.DatastoreCursorWrapper(cursor)
        return results
    
    def update(self, table, key, properties, upsert = False, replace = False):
        properties['_id'] = key
        if replace:
            return self.put(table, properties)
        # When get more fancy, will want transaction.
        document = self.get(table, {'_id' : key})
        for key in properties:
            document[key] = properties[key]
        return self.put(table, document)
    
    
    
    class DuplicateKeyError(Exception):
        """ To pass dup exception through to wrapper.
        """

    class DatastoreCursorWrapper(ndb.QueryIterator):
        """ Allow datastore cursor to munge iterated items.
        """
        def __init__(self, wrapped):
            self._wrapped = wrapped
        def next(self):
            result = self._wrapped.next()
            key_name = result.key.id()
            result_dict = unflatten(result.to_dict())
            result_dict['_id'] = key_name
            return result_dict
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)


