import db
try:
    from gevent import monkey; monkey.patch_all()
except ImportError:
    pass
from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError

class MongoDbWrapper(object):

    def __init__(self, app = None, config = None):
        if app:
            self.init_app(app)
        elif config:
            self._connstr = config['DB_CONNECTION_STRING']
            self._conn = MongoClient(self._connstr)
            self._dbname = config['DB_NAME']
            self._db = self._conn[self._dbname]
            self._ns = config['ENV']
            self._tokenmaps = db.get_tokenmaps(self)
        else:
            raise Exception("app or config please!")

    def init_app(self, app):
        app.extensions['data_wrapper'] = app.extensions.get('data_wrapper', {})
        app.extensions['data_wrapper']['db'] = app.extensions['data_wrapper'].get('db', None)
        if not app.extensions['data_wrapper']['db']:
            self._connstr = app.config['DB_CONNECTION_STRING']
            self._conn = MongoClient(self._connstr)
            self._dbname = app.config['DB_NAME']
            app.extensions['data_wrapper']['db'] = self._conn[self._dbname]
        app.extensions['data_wrapper']['ns'] = app.extensions['data_wrapper'].get('ns', app.config['ENV'])

        self._db = app.extensions['data_wrapper']['db']
        self._ns = app.extensions['data_wrapper']['ns']

        app.extensions['data_wrapper']['tokenmaps'] = db.get_tokenmaps(self)
        self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']

    def drop_db(self, db_name):
        if db_name:
            self._conn.drop_database(db_name)

    def drop_namespace(self, namespace = None):
        db.drop_namespace(self, namespace)

    def refresh_tokenmaps(self):
        self._tokenmaps = db.get_tokenmaps(self)

    def get_token_map(self, table, type):
        try:
            return self._tokenmaps[type][table]
        except:
            return {}

    def init_replication(self, destination_hostname, replication_id = None):
        return db.init_replication(self, destination_hostname, replication_id = replication_id)

    def accept_replicated_batch(self, data):
        return db.accept_replicated_batch(self, data)

    @property
    def config(self):
        return db.get_config(self)

    @property
    def ns(self):
        return self._ns

    def get_collection_names(self):
        ns_collections = self._db.collection_names(include_system_collections=False)
        ns_collections = [collection.replace("%s." % self._ns, '', 1) for collection in ns_collections if collection.startswith("%s." % self._ns)]
        return ns_collections

    def get_collection(self, table):
        ns = self.ns
        collection_name = "%s.%s" % (ns, table)
        return self._db[collection_name]

    @db.unflattener
    @db.flattener
    def get(self, table, properties):
        document = self.get_collection(table).find_one(properties)
        return document

    def remove(self, table, properties):
        return self.get_collection(table).remove(properties)

    @db.flattener
    def prep_document(self, table, document):
        return document

    @db.strong_consistency_option
    def put(self, table, document, replace = False):
        if document is None or table is None:
            return
        document = self.prep_document(table, document)
        try:
            return self.get_collection(table).insert(document)
        except MongoDuplicateKeyError:
            if not replace:
                raise self.DuplicateKeyError('', '')
            else:
                self.remove(table,{'_id':document['_id']})
                return self.get_collection(table).insert(document)

    def put_multi(self, table, documents):
        documents = [self.prep_document(table, document) for document in documents]
        return self.get_collection(table).insert(documents)

    @db.flattener
    def find(self, table, properties, _range = None, sort = [], limit = None, keys_only = False, batch_size = None):
        if _range:
            if 'start' in _range or 'stop' in _range:
                properties[_range['prop']] = {}
            if 'start' in _range:
                properties[_range['prop']]["$gte"] = _range['start']
            if 'stop' in _range:
                properties[_range['prop']]["$lt"] = _range['stop']
        if keys_only:
            cursor = self.get_collection(table, {'_id' : 1}).find(properties)
        else:
            cursor = self.get_collection(table).find(properties)
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)
        if batch_size:
            cursor = cursor.batch_size(batch_size)
        return self.PymongoCursorWrapper(cursor, table = table, token_map = self.get_token_map(table, 'decode'))

    def update(self, table, key, properties, upsert = False, replace = False):
        if table not in ['metadata', 'tokenmaps']:
            properties = db.flatten(properties, token_map = self.get_token_map(table, 'encode'))
        if replace:
            update = properties
        else:
            properties.pop('_id', None)
            update = {"$set" : properties}
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
        def __init__(self, wrapped, table, token_map):
            self._wrapped = wrapped
            self._table = table
            self._token_map = token_map
        def next(self):
            result = self._wrapped.next()
            if self._table not in ['metadata','tokenmaps']:
                result = db.unflatten(result, self._token_map)
            return result
        def __getattr__(self, attr):
            return getattr(self._wrapped, attr)
