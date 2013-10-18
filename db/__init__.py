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

def build_tokenmap(cursor):
    prop_data = {}
    for document in cursor:
        if '_id' in document:
            del document['_id']
        flattened_doc = flatten(document)
        for prop in flattened_doc:
            if prop not in prop_data:
                prop_data[prop] = 0
            prop_data[prop] += 1
    for prop in prop_data:
        prop_data[prop] *= len(prop)
    prop_frequency = sorted(prop_data, key = prop_data.__getitem__, reverse=True)
    token = 0
    token_map = {}
    for prop in prop_frequency:
        token_map[prop] = token
        token += 1
    return token_map

