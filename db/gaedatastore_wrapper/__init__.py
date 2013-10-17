from db import flatten, unflatten
from flask import current_app
from google.appengine.ext import db

class GaeDatastoreWrapper(object):

    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        if 'data_wrapper' not in app.extensions:
            app.extensions['data_wrapper'] = {}


    def create_class(self, name, key_name = None):
        db_class = type(name, (db.Expando,), {})
        if key_name:
            return db_class(key_name = key_name)
        else:
            return db_class()

    def build_query(self, table, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(table)
        query = db.Query(collection)
        results = None
        if key_name:
            from google.appengine.api.datastore import Key
            query.filter('__key__ =', Key.from_path(table, key_name))
        flattened_props = flatten(properties)
        for prop in flattened_props:
            query.filter(prop + ' =', flattened_props[prop])
        return query

    def get(self, table, properties):
        query = self.build_query(table, properties)
        result = query.get()
        if result:
            results = unflatten(db.to_dict(result))
            results['_id'] = result.key().id_or_name()
        return results

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        db.delete(keys_to_delete)

    def put(self, table, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(table, key_name)
        flattened_props = flatten(properties)
        for key in flattened_props:
            setattr(collection, key, flattened_props[key])
        return collection.put().id_or_name()

    def find(self, table, properties, limit = None, keys_only = False):
        query = self.build_query(table, properties)
        results = []
        run = query.run(limit=limit, keys_only = keys_only)
        if keys_only:
            results = list(run)
        else:
            results = self.DatastoreCursorWrapper(run)
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

    class DatastoreCursorWrapper(db._QueryIterator):
        """ Allow datastore cursor to munge iterated items.
        """
        def __init__(self, wrapped):
            self._wrapped = wrapped
        def next(self):
            result = self._wrapped.next()
            key_name = result.key().id_or_name()
            result_dict = unflatten(db.to_dict(result))
            result_dict['_id'] = key_name
            return result_dict
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)


