import types, time, os, sys

""" Not sure where to stick these GAE functions.  Don't really want separate gae_helper module. """

def prep_test_env(env, app_name):
    if env == 'local':
        testbed = gae_import('google.appengine.ext', 'testbed')
        my_testbed = testbed.Testbed()
        my_testbed.activate()
        my_testbed.init_datastore_v3_stub()
        my_testbed.init_memcache_stub()
    elif env == 'remote':
        remote_api_shell = gae_import('remote_api_shell', None)
        remote_api_shell.fix_sys_path()
        from google.appengine.ext.remote_api import remote_api_stub

        def auth_func():
            import getpass
            return (raw_input('Username:'), getpass.getpass('Password:'))

        remote_api_stub.ConfigureRemoteApi(None, '/_ah/remote_api', auth_func, '%s.appspot.com' % (app_name))

def gae_import(module, submodule, retry = True):
    try:
        if not submodule:
            return __import__(module)
        else:
            return getattr(__import__(module, fromlist = [submodule]), submodule)
    except ImportError:
        if not retry:
            raise
        sdk_path = "%s/google_appengine" % (os.environ['HOME'])
        sys.path.insert(0, sdk_path)
        import dev_appserver
        dev_appserver.fix_sys_path()
        if 'google' in sys.modules:
            del sys.modules['google']
        return gae_import(module, submodule, retry = False)

""" """

if 'APPENGINE' in os.environ.keys():
    deferred = gae_import('google.appengine.ext', 'deferred')
else:
    from queue import filesystemqueue as deferred

def get_config(data_class):
    db_class_name = data_class.__class__.__name__
    db_class_folder = db_class_name.lower().replace('wrapper','_wrapper')
    config = {'ENV' : data_class._ns, 'DB_NAME' : getattr(data_class, '_dbname', None), 'DB_CONNECTION_STRING' : getattr(data_class, '_connstr', None), 'DB_CLASS_NAME' : db_class_name, 'DB_CLASS_FOLDER' : db_class_folder}
    return config

def drop_namespace(data_class, namespace = None):
    """ Removes all documents from collections but will leave top level collection name there. """
    original_namespace = data_class._ns
    if namespace:
        data_class._ns = namespace
    try:
        collections = data_class.get_collection_names()
        for collection in collections:
            data_class.remove(collection, {})
    finally:
        data_class._ns = original_namespace

def flatten(props, parent_key = "", token_map = {}):
    new_props = []
    for key, value in props.items():
        # Check for '.', '|' or int and raise exception.
        try:
            int(key)
            raise Exception("Cannot use integers as property names!")
        except ValueError:
            pass
        if '.' in key:
            raise Exception("Cannot have '.' in property names!")
        if '|' in key:
            raise Exception("Cannot have '|' in property names!")
        if key.isdigit():
            raise Exception("Cannot use integers as property names!")
        new_key = parent_key + '|' + key if parent_key else key
        if type(props[key]) is types.DictType:
            new_props.extend(flatten(value, parent_key = new_key, token_map = token_map).items())
        else:
            if new_key in token_map:
                new_key = token_map[new_key]
            new_props.append((new_key, value))
    return dict(new_props)

def unflatten(dictionary, token_map = {}):
    resultDict = dict()
    for key, value in dictionary.iteritems():
        if key in token_map:
            key = token_map[key]
        parts = key.split("|")
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict

def build_metadata(cursor, batch_size = 1000):
    prop_data = {}
    key_count = -1
    key_distribution = []
    keyname = None
    for document in cursor:
        try:
            keyname = document['_id']
            key_count += 1
            if key_count % batch_size == 0:
                key_distribution.append(keyname)
            del document['_id']
        except:
            pass
        flattened_doc = flatten(document)
        for prop in flattened_doc:
            if prop not in prop_data:
                prop_data[prop] = 0
            prop_data[prop] += 1
    for prop in prop_data:
        prop_data[prop] *= len(prop)
    prop_frequency = sorted(prop_data, key = prop_data.__getitem__, reverse=True)
    tokenmap = { prop : str(idx) for idx, prop in enumerate(prop_frequency) }
    return {'tokenmap' : tokenmap, 'key_distribution' : key_distribution, 'batch_size' : batch_size}


def get_tokenmaps(data_class):
    tokenmaps = {'encode' : {}, 'decode' : {}}
    tokenmaps_cursor = data_class.find('tokenmaps', {})
    for tokenmap in tokenmaps_cursor:
        collection_name = tokenmap.pop('_id')
        tokenmaps['encode'][collection_name] = tokenmap
        tokenmaps['decode'][collection_name] = { tokenmaps['encode'][collection_name][prop] : prop for prop in tokenmaps['encode'][collection_name] }
    return tokenmaps

def init_replication(data_class, destination_hostname, replication_id = None):
    if not replication_id:
        replication_id = "%s.%s" % (time.strftime("%Y%m%d%H%M%S", time.localtime()), data_class.ns)

    """ If can find replication_id in replication collection??? check for stop point and continue? raise exception?? """
    collections = data_class.get_collection_names()
    for collection in collections:
        if collection in ['tokenmaps', 'metadata']:
            continue
        deferred.defer(build_replication_metadata, data_class.config, collection, destination_hostname, replication_id, batch_size = 1000)
    return replication_id

def build_replication_metadata(config, collection, destination_hostname, replication_id, batch_size = 1000):
    data_class = get_data_class_from_config(config)
    """ Loop over all documents in collection and build meta. """
    collection_cursor = data_class.find(collection, {}, sort = [('_id', 1)], batch_size = batch_size)
    metadata = build_metadata(collection_cursor, batch_size = batch_size)
    metadata['_id'] = "%s.%s" % (replication_id, collection)
    data_class.put('metadata', metadata, replace = True, consistency = 'STRONG')

    replicate_collection(collection, metadata, replication_id, destination_hostname, data_class)

def replicate_collection(collection, metadata, replication_id, destination_hostname, data_class):
    for idx, key_name in enumerate(metadata['key_distribution']):
        _range = {'prop':'_id'}
        if idx != 0:
            _range['start'] = key_name
        try:
            _range['stop'] = metadata['key_distribution'][idx+1]
        except IndexError:
            pass
        deferred.defer(replicate_batch, data_class.config, collection, metadata, replication_id, _range, destination_hostname)
    return "Replicated!! %s %s %s" % (collection, destination_hostname, replication_id)

def replicate_batch(config, collection, metadata, replication_id, _range, destination_hostname):
    import json, zlib, requests, base64

    data_class = get_data_class_from_config(config)
    document_batch = list(data_class.find(collection, {}, _range = _range, sort = [('_id', 1)], batch_size = metadata['batch_size']))

    data_batch = {'collection' : collection ,'metadata' : metadata, 'replication_id' : replication_id, 'document_batch' : document_batch}
    serialized_batch = json.dumps(data_batch)
    headers = {'Content-Type': 'application/octet-stream', 'Content-Transfer-Encoding' : 'base64'}
    compressed_batch = base64.b64encode(zlib.compress(serialized_batch, 9))
    result = requests.post("http://%s/replicate/batch" % (destination_hostname), data = compressed_batch, headers = headers)
    return "Replicated %s %s %s %s with Result: %s" % (collection, metadata, document_batch, destination_hostname, result)

def get_data_class_from_config(config):
    data_wrapper = __import__('db.' + config['DB_CLASS_FOLDER'], fromlist = [config['DB_CLASS_NAME']])
    _class = getattr(data_wrapper, config['DB_CLASS_NAME'])
    data_class = _class(config = config)
    return data_class

def accept_replicated_batch(data_class, data):
    import json, zlib, base64
    data_batch = json.loads(zlib.decompress(base64.b64decode(data)))
    old_ns = data_class.ns
    """ Currently, this is something like 20130601123015.somenamespace """
    data_class._ns = data_batch['replication_id']
    try:
        tokenmap = data_batch['metadata']['tokenmap']
        if tokenmap:
            tokenmap['_id'] = data_batch['collection']
            data_class.put('tokenmaps', tokenmap, replace = True, consistency = 'STRONG')
            data_class.refresh_tokenmaps()
        for document in data_batch['document_batch']:
            data_class.put(data_batch['collection'], document, replace = True)
    except Exception, e:
        print "Problem accepting batch.  Exception: %s" % (e)
    finally:
        data_class._ns = old_ns
        data_class.refresh_tokenmaps()

def strong_consistency_option(F):
    def wrapped(self, table, document, **kwargs):
        consistency = kwargs.pop('consistency','')
        if consistency == 'STRONG':
            from copy import deepcopy
            document_deepcopy = deepcopy(document)

        put_result = F(self, table, document, **kwargs)

        if consistency == 'STRONG':
            counter = 0
            result = self.get(table, {'_id' : document_deepcopy['_id']})
            while result != document_deepcopy and counter < 3:
                from time import sleep
                counter += 1
                sleep(counter*(0.5))
                result = self.get(table, {'_id' : document_deepcopy['_id']})
            if result != document_deepcopy:
                #raise Exception("Strong consistency was requested but documents do not match!\n%s != %s " % (result, prop_deepcopy))
                put_result = {'result' : put_result, 'verified_read' : False}
            else:
                put_result = {'result' : put_result, 'verified_read' : True}
        return put_result
    return wrapped

def flattener(F):
    def decorated_method(self, table, properties, **kwargs):
        if properties and table not in ['metadata', 'tokenmaps'] and '_flattened' not in kwargs:
            properties =  flatten(properties, token_map = self.get_token_map(table, 'encode'))
        return F(self, table, properties, **kwargs)
    return decorated_method

def unflattener(F):
    def decorated_method(self, table, properties, **kwargs):
        document = F(self, table, properties, **kwargs)
        if document and table not in ['metadata', 'tokenmaps'] and 'keys_only' not in kwargs.keys():
            document = unflatten(document, token_map = self.get_token_map(table, 'decode'))
        return document
    return decorated_method
