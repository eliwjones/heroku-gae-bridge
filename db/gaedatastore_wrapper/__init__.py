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

    def get(self, name, properties, limit=1):
        key_name = properties.pop('_id', None)
        collection = self.create_class(name)
        query = db.Query(collection)
        if key_name:
            from google.appengine.api.datastore import Key
            query.filter('__key__ =', Key.from_path(name, key_name))
        for property in properties:
            query.filter(property[0] + ' =', property[1])
        if limit == 1:
            result = query.get()
            results = db.to_dict(result) if result else None
            results['_id'] = key_name
        else:
            results = []
            run = query.run(limit=limit)
            for result in run:
                results.append(db.to_dict(result))
        return results

    def put(self, name, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(name, key_name)
        for key in properties:
            setattr(collection, key, properties[key])
        return collection.put().id_or_name()
    
    def update(self, table, key, properties, upsert = False, replace = False):
        properties['_id'] = key
        if replace:
            return self.put(table, properties)
        document = self.get(table, {'_id' : key})
        for key in properties:
            document[key] = properties[key]
        return self.put(table, document)
    
    
    
    class DuplicateKeyError(Exception):
        """ To pass dup exception through to wrapper.
        """
