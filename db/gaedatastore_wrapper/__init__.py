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

    def get(self, name, properties, limit=1, for_delete=False):
        key_name = properties.pop('_id', None)
        collection = self.create_class(name)
        query = db.Query(collection)
        results = None
        if key_name:
            from google.appengine.api.datastore import Key
            query.filter('__key__ =', Key.from_path(name, key_name))
        for prop in properties:
            query.filter(prop + ' =', properties[prop])
        if limit == 1:
            result = query.get()
            if result:
                results = unflatten(db.to_dict(result))
                results['_id'] = result.key().id_or_name()
        else:
            results = []
            run = query.run(limit=limit, keys_only = for_delete)
            for result in run:
                if for_delete:
                    results.append(result)
                    continue
                key_name = result.key().id_or_name()
                result_dict = unflatten(db.to_dict(result))
                result_dict['_id'] = key_name
                results.append(result_dict)
        return results

    def remove(self, table, properties):
        results = self.get(table, properties, 1000000, True)
        db.delete(results)

    def put(self, name, properties):
        from db.mongodb_wrapper import mongo_flatten as flatten
        key_name = properties.pop('_id', None)
        collection = self.create_class(name, key_name)
        flattened_props = flatten(properties)
        for key in flattened_props:
            setattr(collection, key, flattened_props[key])
        return collection.put().id_or_name()

    def find(self, table, properties, limit = 1000000):
        return self.get(table, properties, limit)
    
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

def unflatten(dictionary):
    resultDict = dict()
    for key, value in dictionary.iteritems():
        parts = key.split(".")
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict


