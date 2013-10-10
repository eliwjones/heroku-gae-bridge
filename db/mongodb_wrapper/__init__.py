from flask import current_app
from gevent import monkey; monkey.patch_all()
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError
import types

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
        app.extensions['data_wrapper']['ns'] = app.config['ENV']
        app.extensions['data_wrapper']['conn'] = conn
        app.extensions['data_wrapper']['db'] = conn[dbname]
        
    @property
    def db(self):
        return current_app.extensions['data_wrapper']['db']
    
    @property
    def ns(self):
        return current_app.extensions['data_wrapper']['ns']
    
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
        return self.get_collection(table).find(properties)

    def update(self, table, key, properties, upsert = False, replace = False):
        if replace:
            update = properties
        else:
            properties.pop('_id', None)
            flattened_props = mongo_flatten(properties)
            update = {"$set" : flattened_props}
        self.get_collection(table).update({'_id' : key}, update, upsert = upsert)

    def startswith(self, table, starts_with):
        import re
        return self.get_collection(table).find({'_id' : {'$regex' : '^' + re.escape(starts_with)}})

    class DuplicateKeyError(MongoDuplicateKeyError):
        """ To pass dup exception through to wrapper.
        """

# Turns: {"videos" : {"video_1" : {"k1" : "value1"}, "video_2" : {"k1" : "value2"}}}
# Into: {"videos.video_1.k1" : "value1", "videos.video_2.k1" : "value2"}
def mongo_flatten(props, parent_key = ""):
    new_props = []
    for key, value in props.items():
        new_key = parent_key + '.' + key if parent_key else key
        if type(props[key]) is types.DictType:
            new_props.extend(mongo_flatten(value, new_key).items())
        else:
            new_props.append((new_key, value))
    return dict(new_props)