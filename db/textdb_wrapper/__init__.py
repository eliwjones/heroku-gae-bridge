import db
import os, types, json, glob


class TextDbWrapper(object):

    def __init__(self, app=None, config=None):
        if app:
            self.init_app(app)
        elif config:
            self._dbname = config['DB_NAME']
            self._db = os.path.join(os.path.dirname(os.path.abspath(__file__)), config['DB_NAME'])
            self._ns = config['ENV']
            self._tokenmaps = db.get_tokenmaps(self)
        else:
            raise Exception("app or config please!")

    def init_app(self, app):
        app.extensions['data_wrapper'] = app.extensions.get('data_wrapper', {})
        app.extensions['data_wrapper']['db'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), app.config['DB_NAME'])
        app.extensions['data_wrapper']['ns'] = app.extensions['data_wrapper'].get('ns', app.config['ENV'])

        self._dbname = app.config['DB_NAME']
        self._db = app.extensions['data_wrapper']['db']
        self._ns = app.extensions['data_wrapper']['ns']

        app.extensions['data_wrapper']['tokenmaps'] = db.get_tokenmaps(self)
        self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']

    def drop_db(self, db_name):
        import shutil
        try:
            shutil.rmtree(os.path.join(os.path.dirname(os.path.abspath(__file__)), db_name))
        except OSError:
            pass

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
        ns_collections = glob.glob("%s/%s.*" % (self._db, self.ns))
        return [collection.replace("%s/%s." % (self._db, self.ns), '', 1) for collection in ns_collections]

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

    def get_collection(self, table):
        collection_path = "%s/%s.%s" % (self._db, self.ns, table)
        return collection_path

    @db.unflattener
    @db.flattener
    def get(self, table, properties):
        document = None
        collectionpath = self.get_collection(table)
        try:
            if '_id' not in properties:
                result = self.find(table, properties, limit = 1, _flattened = True)
                document = list(result)[0]
            else:
                with open("%s/%s" % (collectionpath, properties["_id"])) as file:
                    result = file.read()
                document = json.loads(result)
        except:
            pass
        return document

    @db.strong_consistency_option
    @db.flattener
    def put(self, table, document, replace = False, **kwargs):
        if document is None or table is None:
            return
        collectionpath = self.get_collection(table)
        if not replace and os.path.exists(collectionpath + '/' + document['_id']):
            raise self.DuplicateKeyError
        try:
            with open(collectionpath + '/' + document['_id'], 'w') as file:
                file.write(json.dumps(document))
        except:
            if not os.path.exists(collectionpath):
                os.makedirs(collectionpath)
                return self.put(table, document, _flattened = True)
            raise
        return document['_id']

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        for filename in keys_to_delete:
            try:
                os.remove(filename)
            except:
                pass

    @db.flattener
    def find(self, table, properties, _range = None, sort = [], limit = None, keys_only = False, **kwargs):
        matches = 0
        collectionpath = self.get_collection(table)
        documents = []
        document_paths = glob.glob(collectionpath + '/*')
        for document_path in document_paths:
            match = True
            try:
                with open(document_path, 'r') as document_json:
                    document = json.loads(document_json.read())
                    if _range and not (_range['start'] <= document[_range['prop']] < _range['stop']):
                        raise Exception("Not matched!")
                    for prop_key in properties:
                        if properties[prop_key] != document[prop_key]:
                            raise Exception("Not matched!")
            except:
                match = False
            if match:
                matches += 1
                if keys_only:
                    document = document_path
                documents.append(document)
                if matches == limit:
                    break
        if keys_only:
            return documents
        else:
            if sort:
                def key_function(item):
                    str_key = ''
                    for sort_info in sort:
                        if sort_info[0] in item:
                            sort_val = item[sort_info[0]]
                            if sort_info[1] == -1:
                                # Hacky to reflect characters to other side of spectrum.
                                sort_val = "".join(chr(127 - (ord(letter) - 128)) for letter in sort_val)
                            str_key = "%s_%s" % (str_key, sort_val)
                    return str_key
                documents = sorted(documents, key = key_function)
            return self.TextDbCursorWrapper(iter(documents), table = table, token_map = self.get_token_map(table, 'decode'))


    def update(self, table, key, properties, upsert = False, replace = False):
        document = self.get(table, {'_id' : key})
        if replace:
            document = {'_id' : key}
        if upsert and document is None:
            document = {'_id' : key}
        updated_document = merge_dicts(document, properties)
        self.put(table, updated_document, replace = True)

    def startswith(self, table, starts_with):
        collectionpath = self.get_collection(table)
        document_paths = glob.glob('%s/%s*' % (collectionpath, starts_with))
        documents = []
        for document_path in document_paths:
            with open(document_path, 'r') as document_json:
                document = json.loads(document_json.read())
                if document:
                    documents.append(document)
        return documents

    class DuplicateKeyError(Exception):
        """ To pass dup exception through to wrapper.
        """

    from collections import Iterator
    class TextDbCursorWrapper(Iterator):
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

def merge_dicts(x,y):
    merged = dict(x,**y)
    xkeys = x.keys()
    for key in xkeys:
        if type(x[key]) is types.DictType and y.has_key(key):
            merged[key] = merge_dicts(x[key],y[key])
    return merged
