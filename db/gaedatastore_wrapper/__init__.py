import db
from google.appengine.ext import ndb

class GaeDatastoreWrapper(object):

    def __init__(self, app = None, config = None):
        if app:
            self.init_app(app)
        elif config:
            self._ns = config['ENV']
            self._tokenmaps = db.get_tokenmaps(self)
        else:
            raise Exception("app or config please!")

    def init_app(self, app):
        app.extensions['data_wrapper'] = app.extensions.get('data_wrapper', {})
        app.extensions['data_wrapper']['ns'] = app.extensions['data_wrapper'].get('ns', app.config['ENV'])
        self._ns = app.extensions['data_wrapper']['ns']
        app.extensions['data_wrapper']['tokenmaps'] = app.extensions['data_wrapper'].get('tokenmaps', db.get_tokenmaps(self))
        self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']
            
    def refresh_tokenmaps(self):
        self._tokenmaps = db.get_tokenmaps(self)

    def get_token_map(self, table, type):
        try:
            return self._tokenmaps[type][table]
        except:
            return {}

    def get_collection_names(self):
        from google.appengine.ext.ndb.metadata import Kind
        query = ndb.Query(kind = '__kind__', namespace = self.ns)
        return [collection.kind_name for collection in query.fetch()]

    def init_replication(self, destination_hostname):
        return db.init_replication(self, destination_hostname)
    
    def accept_replicated_batch(self, data):
        return db.accept_replicated_batch(self, data)
    
    @property
    def config(self):
        return db.get_config(self)

    @property
    def ns(self):
        return self._ns

    def create_class(self, name, key_name = None):
        db_class = type(str(name), (ndb.Expando,), {})
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
        flattened_props = db.flatten(properties, token_map = self.get_token_map(table, 'encode'))
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
            result = db.unflatten(result.to_dict(), token_map = self.get_token_map(table, 'decode'))
            result['_id'] = key_name
        return result

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        ndb.delete_multi(keys_to_delete)

    def put(self, table, properties):
        key_name = properties.pop('_id', None)
        collection = self.create_class(table, key_name)
        if table not in ['metadata', 'tokenmaps']:
            properties = db.flatten(properties, token_map = self.get_token_map(table, 'encode'))
        for key in properties:
            setattr(collection, key, properties[key])
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
            results = self.DatastoreCursorWrapper(cursor, token_map = self.get_token_map(table, 'decode'))
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
        def __init__(self, wrapped, token_map):
            self._wrapped = wrapped
            self._token_map = token_map
        def next(self):
            result = self._wrapped.next()
            key_name = result.key.id()
            result_dict = db.unflatten(result.to_dict(), token_map = self._token_map)
            result_dict['_id'] = key_name
            return result_dict
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)


