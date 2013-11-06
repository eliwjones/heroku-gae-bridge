from db.abstract_tests import AbstractTest

import db, pytest
env = pytest.config.getoption('--env')

if env == 'local':
    testbed = db.gae_import('google.appengine.ext', 'testbed')
    my_testbed = testbed.Testbed()
    my_testbed.activate()
    my_testbed.init_datastore_v3_stub()
    my_testbed.init_memcache_stub()
elif env == 'remote':
    remote_api_shell = db.gae_import('remote_api_shell', None)
    remote_api_shell.fix_sys_path()

    from google.appengine.api import datastore_admin
    from google.appengine.ext.remote_api import remote_api_stub
    from google.appengine.tools import appengine_rpc

    def auth_func():
        import getpass
        return (raw_input('Username:'), getpass.getpass('Password:'))

    remote_api_stub.ConfigureRemoteApi(None, '/_ah/remote_api', auth_func, 'herokugaehello.appspot.com')

from db.gaedatastore_wrapper import GaeDatastoreWrapper

class TestGaeDatastore(AbstractTest):
    data_wrapper = GaeDatastoreWrapper
