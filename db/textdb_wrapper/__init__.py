from flask import current_app
import os, types, json


class TextDbWrapper(object):
    
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)
 
    def init_app(self, app):
        if 'data_wrapper' not in app.extensions:
            app.extensions['data_wrapper'] = {}
        
        conn = {}
        dbname = "tf" # app.name # Nets out to 'tf'?
        app.extensions['data_wrapper']['conn'] = conn
        app.extensions['data_wrapper']['db'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), dbname)
        
    @property
    def db(self):
        return current_app.extensions['data_wrapper']['db']


    def get(self, table, properties = None):
        try:
            with open("%s/%s/%s" % (self.db, table, properties["_id"])) as file:
                result = file.read()
            return json.loads(result)
        except:
            return None

    def put(self, table, document, replace = False):
        if document is None or table is None:
            return
        filepath = "%s/%s" % (self.db, table)
        if not replace and os.path.exists(filepath + '/' + document['_id']):
            raise self.DuplicateKeyError
        try:
            with open(filepath + '/' + document['_id'], 'w') as file:
                file.write(json.dumps(document))
        except:
            if not os.path.exists(filepath):
                os.makedirs(filepath)
                return self.put(table, document)
            raise
        return document['_id']

    def remove(self, table, key):
        if not table or not key:
            return None
        filename = "%s/%s/%s" % (self.db, table, key)
        os.remove(filename)

    def find(self, table, properties):
        try:
            return self.db[table]
        except:
            return None

    def update(self, table, key, properties, upsert = False, replace = False):
        document = self.get(table, {'_id' : key})
        if replace and document is not None:
            document = {'_id' : key}
        if upsert and document is None:
            document = {'_id' : key}
        updated_document = merge_dicts(document, properties)
        self.put(table, updated_document, True)

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
