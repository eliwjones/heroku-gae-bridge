# gaedatastore_wrapper pre-amble

import sys
sys.path.insert(0, '../')
sdk_path = '/home/mrz/google_appengine'
sys.path.insert(0, sdk_path)
import dev_appserver
dev_appserver.fix_sys_path()

from google.appengine.ext import testbed
my_testbed = testbed.Testbed()
my_testbed.activate()
my_testbed.init_datastore_v3_stub()
my_testbed.init_memcache_stub()

data_classes = {'gaedatastore_wrapper':'GaeDatastoreWrapper', 'mongodb_wrapper':'MongoDbWrapper', 'textdb_wrapper': 'TextDbWrapper'}
for data_class_folder in data_classes:
    print "******** Testing: %s ********" % (data_class_folder)

    config = {'DB_CONNECTION_STRING' : None, 'DB_NAME' : 'app', 'ENV' : 'sandbox'}
    data_wrapper = __import__('db.' + data_class_folder, fromlist = [data_classes[data_class_folder]])
    _class = getattr(data_wrapper, data_classes[data_class_folder])
    data_class = _class(config = config)

    data_class.remove('test_collection', {})
    data_class.remove('tokenmaps', {})

    # Test put, get, find, update, remove
    base_document1 = {'_id' : 'my_test_id1', 'string_property' : 'string_value', 'integer_property' : 10, 'nested' : {'thing' : 'in_nest'}}
    from copy import deepcopy
    props = deepcopy(base_document1)

    result = data_class.put('test_collection', props)

    result = data_class.get('test_collection', props)
    #print "RESULTS: %s" % (result)
    print "GET: %s" % (result == base_document1)

    result = list(data_class.find('test_collection', {'_id' : 'my_test_id1'}))
    #print "RESULTS: %s" % (result)
    print "FIND: %s" %  (result == [base_document1])

    base_document2 = deepcopy(props)
    base_document2['_id'] = 'my_test_id2'
    props2 = deepcopy(base_document2)

    data_class.put('test_collection', props2)
    result = data_class.get('test_collection', {'_id' : 'my_test_id2'})
    print "PUT: %s" % (result == base_document2)

    data_class.update('test_collection', 'my_test_id2', {'nested':{'thing' : 'nested was updated'}})
    updated_doc2 = deepcopy(base_document2)
    updated_doc2['nested']['thing'] = 'nested was updated'
    result = data_class.get('test_collection', {'_id' : 'my_test_id2'})
    print "UPDATE: %s" % (result == updated_doc2)

    result = list(data_class.find('test_collection', {'string_property' : 'string_value'}))
    result.sort()
    comp_list = [base_document1, updated_doc2]
    comp_list.sort()
    #print "RESULTS: %s" % (result)
    print "FIND2: %s" % (result == comp_list)

    result = data_class.remove('test_collection', {'_id' : 'my_test_id1'})
    result = list(data_class.find('test_collection',{}))
    #print "RESULTS: %s" % (result)
    print "REMOVE: %s" % (result == [updated_doc2])

    # Test Token Mapping #
    from db import unflatten
    data_class.put('tokenmaps', unflatten({'_id' : 'test_collection', 'new|nested|thing' : '0'}))
    data_class.refresh_tokenmaps()
    print "** TOKEN MAPS **"
    print "%s" % (data_class._tokenmaps)

    print "'test_collection' encode tokenmap: %s" % (data_class.get_token_map('test_collection', 'encode'))
    print "'test_collection' decode tokenmap: %s" % (data_class.get_token_map('test_collection', 'decode'))
    data_class.put('test_collection', {'_id' : 'tokenized_id1', 'new' : {'nested':{'thing': 'this_is deeply nested and tokenized'}}})
    result = data_class.get('test_collection', {'_id' : 'tokenized_id1'})
    print result

    data_class.remove('tokenmaps', {})
#    data_class.remove('test_collection', {})

my_testbed.deactivate()
