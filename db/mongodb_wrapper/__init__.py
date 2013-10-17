from db import flatten
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
 
    def init_app(self, app):
        if 'data_wrapper' not in app.extensions:
            app.extensions['data_wrapper'] = {}
        connstr = app.config['DB_CONNECTION_STRING']
        conn = MongoClient(connstr)
        dbname =  app.config['DB_NAME']
        self._db = conn[dbname]
        self._ns = app.config['ENV']
        
    @property
    def db(self):
        return self._db
    
    @property
    def ns(self):
        return self._ns
    
    def get_collection(self, table):
        ns = self.ns
        collection_name = "%s.%s" % (ns, table)
        return self.db[collection_name]
    
    def get(self, table, properties = None):
        return self.get_collection(table).find_one(properties)
    
    def remove(self, table, properties):
        return self.get_collection(table).remove(properties)
    
    def put(self, table, document):
        if document is None or table is None:
            return
        try:
            return self.get_collection(table).insert(document)
        except MongoDuplicateKeyError:
            raise self.DuplicateKeyError('', '')

    def find(self, table, properties):
        cursor = self.get_collection(table).find(properties)
        return self.PymongoCursorWrapper(cursor)

    def update(self, table, key, properties, upsert = False, replace = False):
        if replace:
            update = properties
        else:
            properties.pop('_id', None)
            flattened_props = flatten(properties)
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
        def __init__(self, wrapped):
            self._wrapped = wrapped
        def next(self):
            result = self._wrapped.next()
            return result
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)
