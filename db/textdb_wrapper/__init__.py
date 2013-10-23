from db import flatten, unflatten, get_tokenmaps
import os, types, json, glob


class TextDbWrapper(object):
    
    def __init__(self, app=None, config=None):
        if app:
            self.init_app(app)
        elif config:
            self._db = os.path.join(os.path.dirname(os.path.abspath(__file__)), config['DB_NAME'])
            self._ns = config['ENV']
            self._tokenmaps = get_tokenmaps(self)
        else:
            raise Exception("app or config please!")
 
    def init_app(self, app):
        app.extensions['data_wrapper'] = app.extensions.get('data_wrapper', {})
        app.extensions['data_wrapper']['db'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), app.config['DB_NAME'])
        app.extensions['data_wrapper']['ns'] = app.extensions['data_wrapper'].get('ns', app.config['ENV'])
        
        self._db = app.extensions['data_wrapper']['db']
        self._ns = app.extensions['data_wrapper']['ns']
        
        app.extensions['data_wrapper']['tokenmaps'] = app.extensions['data_wrapper'].get('tokenmaps', get_tokenmaps(self))
        self._tokenmaps = app.extensions['data_wrapper']['tokenmaps']
        
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
    
    def get_collection(self, table):
        collection_path = "%s/%s.%s" % (self.db, self.ns, table)
        return collection_path

    def get(self, table, properties = None):
        document = None
        if properties:
            properties = flatten(properties, token_map = self.get_token_map(table, 'encode'))
        try:
            collectionpath = self.get_collection(table)
            with open("%s/%s" % (collectionpath, properties["_id"])) as file:
                result = file.read()
            document = json.loads(result)
        except:
            pass
        if document:
            document = unflatten(document, token_map = self.get_token_map(table, 'decode'))
        return document

    def put(self, table, document, replace = False):
        if document is None or table is None:
            return
        document = flatten(document, token_map = self.get_token_map(table, 'encode'))
        collectionpath = self.get_collection(table)
        if not replace and os.path.exists(collectionpath + '/' + document['_id']):
            raise self.DuplicateKeyError
        try:
            with open(collectionpath + '/' + document['_id'], 'w') as file:
                file.write(json.dumps(document))
        except:
            if not os.path.exists(collectionpath):
                os.makedirs(collectionpath)
                return self.put(table, unflatten(document, token_map = self.get_token_map(table, 'decode')))
            raise
        return document['_id']

    def remove(self, table, properties):
        keys_to_delete = self.find(table, properties, keys_only = True)
        for filename in keys_to_delete:
            try:
                os.remove(filename)
            except:
                pass

    def find(self, table, properties, keys_only = False):
        properties = flatten(properties, token_map = self.get_token_map(table, 'encode'))
        collectionpath = self.get_collection(table)
        documents = []
        document_paths = glob.glob(collectionpath + '/*')
        for document_path in document_paths:
            match = True
            try:
                with open(document_path, 'r') as document_json:
                    document = json.loads(document_json.read())
                    for prop_key in properties:
                        if properties[prop_key] != document[prop_key]:
                            match = False
            except:
                match = False
            if match:
                if keys_only:
                    document = document_path
                else:
                    document = unflatten(document, token_map = self.get_token_map(table, 'decode'))
                documents.append(document)
        return documents
        

    def update(self, table, key, properties, upsert = False, replace = False):
        document = self.get(table, {'_id' : key})
        if replace and document is not None:
            document = {'_id' : key}
        if upsert and document is None:
            document = {'_id' : key}
        updated_document = merge_dicts(document, properties)
        self.put(table, updated_document, replace = True)

    def startswith(self, table, starts_with):
        import glob
        filepath = "%s/%s" % (self.db, table)
        results = ""
        for filename in glob.glob("%s/%s*" % (filepath, starts_with)):
            with open(filename) as file:
                results = results + file.read().rstrip()
                results = results + ','
        results = '[' + results[:-1] + ']'
        return json.loads(results)

    class DuplicateKeyError(Exception):
        """ To pass dup exception through to wrapper.
        """

def merge_dicts(x,y):
    merged = dict(x,**y)
    xkeys = x.keys()
    for key in xkeys:
        if type(x[key]) is types.DictType and y.has_key(key):
            merged[key] = merge_dicts(x[key],y[key])
    return merged
