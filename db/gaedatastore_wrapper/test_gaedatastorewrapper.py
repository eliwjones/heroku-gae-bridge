from db.abstract_tests import AbstractTest

import db, pytest
env = pytest.config.getoption('--env')  # @UndefinedVariable

db.prep_test_env(env, app_name = 'herokugaehello')
from db.gaedatastore_wrapper import GaeDatastoreWrapper

class TestGaeDatastore(AbstractTest):
    data_wrapper = GaeDatastoreWrapper
