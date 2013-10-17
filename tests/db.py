# gaedatastore_wrapper pre-amble

import sys
sys.path.insert(0, '../')
sdk_path = '/home/mrz/google_appengine'
sys.path.insert(0, sdk_path)
import dev_appserver
dev_appserver.fix_sys_path()

from google.appengine.ext import db
from google.appengine.ext import testbed

my_testbed = testbed.Testbed()

data_classes = {'gaedatastore_wrapper':'GaeDatastoreWrapper', 'mongodb_wrapper':'MongoDbWrapper'}
for data_class_folder in data_classes:
    print "******** Testing: %s ********" % (data_class_folder)
    my_testbed.activate()
    my_testbed.init_datastore_v3_stub()

    app = type('App', (object,), {})
    app.extensions = {}
    app.config = {'DB_CONNECTION_STRING' : None, 'DB_NAME' : 'app', 'ENV' : 'sandbox'}

    data_wrapper = __import__('db.' + data_class_folder, fromlist = [data_classes[data_class_folder]])
    _class = getattr(data_wrapper, data_classes[data_class_folder])
    data_class = _class(app)

    # Test put, get, find, update, remove
    base_document1 = {'_id' : 'my_test_id1', 'string_property' : 'string_value', 'integer_property' : 10, 'nested' : {'thing' : 'in_nest'}}
    from copy import deepcopy
    props = deepcopy(base_document1)

    result = data_class.put('test_collection', props)

    result = data_class.get('test_collection', props)
    print "GET"
    print result == base_document1

    print "FIND"
    result = list(data_class.find('test_collection', {'_id' : 'my_test_id1'}))
    print result == [base_document1]

    base_document2 = deepcopy(props)
    base_document2['_id'] = 'my_test_id2'
    props2 = deepcopy(base_document2)

    data_class.put('test_collection', props2)
    print "FIND2"
    result = list(data_class.find('test_collection', {'string_property' : 'string_value'}))
    result.sort()
    comp_list = [base_document1, base_document2]
    comp_list.sort()
    print result == comp_list


    result = data_class.remove('test_collection', {'_id' : 'my_test_id1'})
    result = list(data_class.find('test_collection',{}))
    print result == [base_document2]

    data_class.remove('test_collection', {})

    my_testbed.deactivate()
