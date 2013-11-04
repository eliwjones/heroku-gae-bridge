from db.abstract_tests import AbstractTest
from db.mongodb_wrapper import MongoDbWrapper

class TestMongoDb(AbstractTest):
    data_wrapper = MongoDbWrapper
