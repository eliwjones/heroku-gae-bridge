from db.abstract_tests import AbstractTest

import dev_appserver
dev_appserver.fix_sys_path()

from google.appengine.ext import testbed
my_testbed = testbed.Testbed()
my_testbed.activate()
my_testbed.init_datastore_v3_stub()
my_testbed.init_memcache_stub()

from db.gaedatastore_wrapper import GaeDatastoreWrapper

class TestGaeDatastore(AbstractTest):
    data_wrapper = GaeDatastoreWrapper
