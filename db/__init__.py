import types

def flatten(props, parent_key = ""):
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
            new_props.extend(flatten(value, new_key).items())
        else:
            new_props.append((new_key, value))
    return dict(new_props)

def unflatten(dictionary):
    resultDict = dict()
    for key, value in dictionary.iteritems():
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
                key_distribution[key_count] = keyname
            del document['_id']
        except:
            pass
        flattened_doc = flatten(document)
        for prop in flattened_doc:
            if prop not in prop_data:
                prop_data[prop] = 0
            prop_data[prop] += 1
    # Set final keyname in distribution.
    key_distribution[key_count] = keyname
    for prop in prop_data:
        prop_data[prop] *= len(prop)
    prop_frequency = sorted(prop_data, key = prop_data.__getitem__, reverse=True)
    token_map = {prop:idx for idx,prop in enumerate(prop_frequency)}
    return {'token_map' : token_map, 'key_distribution' : key_distribution, 'partition_size' : partition_size}


def get_tokenmaps(data_class):
    tokenmaps = {}
    tokenmaps_cursor = data_class.find('tokenmaps', {})
    for tokenmap in tokenmaps_cursor:
        collection_name = tokenmap.pop('_id')
        tokenmaps[collection_name] = {}
        tokenmaps[collection_name]['encode'] = flatten(tokenmap)
        tokenmaps[collection_name]['decode'] = { tokenmaps[collection_name]['encode'][prop] : prop for prop in tokenmaps[collection_name]['encode'] }
    return tokenmaps
