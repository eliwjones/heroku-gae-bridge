import types

def flatten(props, parent_key = ""):
    new_props = []
    for key, value in props.items():
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
