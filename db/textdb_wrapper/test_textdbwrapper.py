from db.abstract_tests import AbstractTest
from db.textdb_wrapper import TextDbWrapper

class TestTextDb(AbstractTest):
    data_wrapper = TextDbWrapper