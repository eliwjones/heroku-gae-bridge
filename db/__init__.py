import types, time, os
if 'APPENGINE' in os.environ.keys():
    from google.appengine.ext import deferred
else:
    from queue import filesystemqueue as deferred

def get_config(data_class):
    db_class_name = data_class.__class__.__name__
    db_class_folder = db_class_name.lower().replace('wrapper','_wrapper')
    config = {'ENV' : data_class._ns, 'DB_NAME' : getattr(data_class, '_dbname', None), 'DB_CONNECTION_STRING' : getattr(data_class, '_connstr', None), 'DB_CLASS_NAME' : db_class_name, 'DB_CLASS_FOLDER' : db_class_folder}
    return config

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

def build_metadata(cursor):
    prop_data = {}
    key_count = -1
    partition_size = 1000
    key_distribution = {}
    keyname = None
    for document in cursor:
        try:
            keyname = document['_id']
            key_count += 1
            if key_count % partition_size == 0:
                key_distribution[str(key_count)] = keyname
            del document['_id']
        except:
            pass
        flattened_doc = flatten(document)
        for prop in flattened_doc:
            if prop not in prop_data:
                prop_data[prop] = 0
            prop_data[prop] += 1
    # Set final keyname in distribution.
    key_distribution[str(key_count)] = keyname
    for prop in prop_data:
        prop_data[prop] *= len(prop)
    prop_frequency = sorted(prop_data, key = prop_data.__getitem__, reverse=True)
    tokenmap = { prop : str(idx) for idx, prop in enumerate(prop_frequency) }
    return {'tokenmap' : tokenmap, 'key_distribution' : key_distribution, 'partition_size' : partition_size}


def get_tokenmaps(data_class):
    tokenmaps = {'encode' : {}, 'decode' : {}}
    tokenmaps_cursor = data_class.find('tokenmaps', {})
    for tokenmap in tokenmaps_cursor:
        collection_name = tokenmap.pop('_id')
        tokenmaps['encode'][collection_name] = flatten(tokenmap)
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
        deferred.defer(build_replication_metadata, data_class.config, collection, destination_hostname, replication_id)
    return replication_id

def build_replication_metadata(config, collection, destination_hostname, replication_id):
    data_class = get_data_class_from_config(config)
    """ Loop over all documents in collection and build meta. """
    metadata = build_metadata(data_class.find(collection, {}))
    metadata['_id'] = "%s.%s" % (replication_id, collection)
    data_class.put('metadata', metadata, replace = True, consistency = 'STRONG')

    """ Fire off async task to replicate collection. """
    deferred.defer(replicate_collection, collection, metadata, replication_id, destination_hostname, data_class.config)
    return "replicate_collection call was deferred!"

def replicate_collection(collection, metadata, replication_id, destination_hostname, config):
    data_class = get_data_class_from_config(config)
    
    """ Chunk into batches of 100 or 1000 and track progress? """
    document_batch = []
    batch_size = 1000
    collection_cursor = data_class.find(collection, {})
    for idx, document in enumerate(collection_cursor):
        document_batch.append(document)
        if idx%1000 and idx > 0:
            replicate_batch(collection, metadata, replication_id, document_batch, destination_hostname)
            document_batch = []
    if document_batch:
        replicate_batch(collection, metadata, replication_id, document_batch, destination_hostname)
    return "Replicated!! %s %s %s" % (collection, destination_hostname, replication_id)

def replicate_batch(collection, metadata, replication_id, document_batch, destination_hostname):
    import json, zlib, requests, base64
    """
      1. serialize and compress batch.
      2. post to replicate endpoint on destination_hostname.
      3. Track?? Or just look for missing batches later?
      4. destination host should report back on received batches.
    """
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