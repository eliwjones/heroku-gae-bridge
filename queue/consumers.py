from db import build_replication_metadata, replicate_collection, replicate_batch

def read_textdb_func(collection, properties, config):
    from db.textdb_wrapper import TextDbWrapper
    data_class = TextDbWrapper(config = config)
    result = data_class.get(collection, properties)
    return result

def my_func(thangs):
    return "THANGS: %s" % (thangs)
