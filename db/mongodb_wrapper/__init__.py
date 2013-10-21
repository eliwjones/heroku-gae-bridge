from db import flatten, unflatten, get_tokenmaps
try:
    from gevent import monkey; monkey.patch_all()
except ImportError:
    pass
from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError

class MongoDbWrapper(object):
    
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)
            self._db = app.extensions['data_wrapper']['db']
            self._ns = app.extensions['data_wrapper']['ns']
            if 'tokenmaps' not in app.extensions['data_wrapper']:
                app.extensions['data_wrapper']['tokenmaps'] = get_tokenmaps(self)
            self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']
 
    def init_app(self, app):
        if 'data_wrapper' not in app.extensions:
            app.extensions['data_wrapper'] = {}
            connstr = app.config['DB_CONNECTION_STRING']
            conn = MongoClient(connstr)
            dbname =  app.config['DB_NAME']
            app.extensions['data_wrapper']['ns'] = app.config['ENV']
            app.extensions['data_wrapper']['db'] = conn[dbname]

    def refresh_tokenmaps(self):
        self._tokenmaps = get_tokenmaps(self)

    def get_token_map(self, table, type):
        try:
            return self._tokenmaps[type][table]
        except:
            return {}

    @property
    def db(self):
        return self._db
    
    @property
    def ns(self):
        return self._ns

    def get_collection_names(self):
        ns_collections = self.db.collection_names(include_system_collections=False)
        ns_collections = [collection.replace("%s." % self._ns, '', 1) for collection in ns_collections if collection.startswith("%s." % self._ns)]
        return ns_collections
    
    def get_collection(self, table):
        ns = self.ns
        collection_name = "%s.%s" % (ns, table)
        return self.db[collection_name]
    
    def get(self, table, properties = None):
        if properties:
            properties = flatten(properties, token_map = self.get_token_map(table, 'encode'))
        document = self.get_collection(table).find_one(properties)
        if document:
            document = unflatten(document, token_map = self.get_token_map(table, 'decode'))
        return document
    
    def remove(self, table, properties):
        return self.get_collection(table).remove(properties)
    
    def put(self, table, document):
        if document is None or table is None:
            return
        try:
            document = flatten(document, token_map = self.get_token_map(table, 'encode'))
            return self.get_collection(table).insert(document)
        except MongoDuplicateKeyError:
            raise self.DuplicateKeyError('', '')

    def find(self, table, properties):
        properties = flatten(properties, token_map = self.get_token_map(table, 'encode'))
        cursor = self.get_collection(table).find(properties)
        return self.PymongoCursorWrapper(cursor, token_map = self.get_token_map(table, 'decode'))

    def update(self, table, key, properties, upsert = False, replace = False):
        if replace:
            update = flatten(properties, token_map = self.get_token_map(table, 'encode'))
        else:
            properties.pop('_id', None)
            flattened_props = flatten(properties, token_map = self.get_token_map(table, 'encode'))
            update = {"$set" : flattened_props}
        self.get_collection(table).update({'_id' : key}, update, upsert = upsert)

    def startswith(self, table, starts_with):
        import re
        return self.get_collection(table).find({'_id' : {'$regex' : '^' + re.escape(starts_with)}})

    class DuplicateKeyError(MongoDuplicateKeyError):
        """ To pass dup exception through to wrapper.
        """

    class PymongoCursorWrapper(Cursor):
        """ Mostly for proof-of-concept. Needed in GaeDatastoreWrapper.
        """ 
        def __init__(self, wrapped, token_map):
            self._wrapped = wrapped
            self._token_map = token_map
        def next(self):
            result = self._wrapped.next()
            return unflatten(result, self._token_map)
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)
