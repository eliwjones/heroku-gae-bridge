import db
ndb = db.gae_import('google.appengine.ext', 'ndb')

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
        app.extensions['data_wrapper']['tokenmaps'] = db.get_tokenmaps(self)
        self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']

    def drop_namespace(self, namespace = None):
        db.drop_namespace(self, namespace)

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

    @staticmethod
    def create_class(name):
        return type(str(name), (ndb.Expando,), {})

    @db.flattener
    def build_query(self, table, properties, _range = None, sort = []):
        model_class = self.create_class(table)
        key_name = properties.pop('_id', None)
        if key_name:
            return ndb.Key(model_class, key_name, namespace = self._ns)
        query = model_class.query(namespace = self._ns)
        for prop in properties:
            query = query.filter(ndb.GenericProperty(prop) == properties[prop])
        if _range:
            if _range['prop'] == '_id':
                range_prop = model_class.key
                _range['start'] = ndb.Key(table, _range['start'], namespace = self._ns)
                _range['stop'] = ndb.Key(table, _range['stop'], namespace = self._ns)
            else:
                range_prop = ndb.GenericProperty(_range['prop'])
            query = query.filter(range_prop >= _range['start']).filter(range_prop < _range['stop'])
        for sort_info in sort:
            if sort_info[0] == '_id':
                sort_prop = model_class.key
            else:
                sort_prop = ndb.GenericProperty(sort_info[0])
            if sort_info[1] == -1:
                query = query.order(-sort_prop)
            else:
                query = query.order(sort_prop)
        return query

    @db.unflattener
    def get(self, table, properties, keys_only = False):
        query = self.build_query(table, properties)
        result = query.get()
        if result:
            if keys_only:
                return result.key
            key_name = result.key.id()
            result = result.to_dict()
            result['_id'] = key_name
        return result

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        ndb.delete_multi(keys_to_delete)

    @db.strong_consistency_option
    @db.flattener
    def put(self, table, document):
        if document is None or table is None:
            return
        key_name = document.pop('_id', None)
        collection = self.create_class(table)(id = key_name, namespace = self._ns)
        for key in document:
            setattr(collection, key, document[key])
        return collection.put().id()

    def find(self, table, properties, _range = None, sort = [], limit = None, keys_only = False):
        if '_id' in properties:
            return [self.get(table, properties, keys_only = keys_only)]
        query = self.build_query(table, properties, _range = _range, sort = sort)
        results = []
        cursor = query.iter(limit=limit, keys_only = keys_only)
        if keys_only:
            results = cursor
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

    def startswith(self, table, starts_with):
        final_char_ord = ord(starts_with[-1])
        if final_char_ord < 127:
            stops_with = starts_with[:-1] + chr(final_char_ord + 1)
        else:
            stops_with = starts_with + (500-len(starts_with))*chr(127)
        return self.find(table, {}, _range = {'prop' : '_id', 'start' : starts_with, 'stop' : stops_with})

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


