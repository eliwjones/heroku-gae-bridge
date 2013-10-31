import db
from db.textdb_wrapper import TextDbWrapper

class TestDb:
    @classmethod
    def setup_class(self):
        self.config = {'DB_CONNECTION_STRING' : None, 'DB_NAME' : 'testdb', 'ENV' : 'testenv'}
        self.data_class = TextDbWrapper(config = self.config)
        self.data_class.drop_db(self.config['DB_NAME'])

    @classmethod
    def teardown_class(self):
        self.data_class.drop_db(self.config['DB_NAME'])

    def test_db_flatten(self):
        document = {'_id' : 'keyname1', 'nested' : {'item':1}}
        assert db.flatten(document) == {'_id' : 'keyname1', 'nested|item' : 1}
        document = {'_id' : 'keyname1', 'notnested' : 'a'}
        assert db.flatten(document) == document

    def test_db_unflatten(self):
        document = {'_id' : 'keyname1', 'deeply|nested|item' : 1}
        assert db.unflatten(document) == {'_id' : 'keyname1', 'deeply' : { 'nested' : { 'item' : 1} } }

    def test_db_flatten_unflatten(self):
        document = {'_id' : 'keyname1', 'nested' : {'item':1}}
        assert db.unflatten(db.flatten(document)) == document
        
    def test_build_metadata(self):
        cursor = [{'_id' : 'keyname1', 'abcdefg' : 1, 'ab' : {'cd' : 1}},
                  {'_id' : 'keyname2', 'abcdefg' : 1, 'abcd': 1 }]
        metadata = db.build_metadata(cursor)
        assert metadata['tokenmap'] == {'abcdefg':'0', 'ab|cd' : '1', 'abcd':'2'}
        
    def test_get_tokenmaps(self):
        result = self.data_class.put('tokenmaps', {'_id' : 'test_collection', 'abcdefg' : '0', 'ab|cd' : '1', 'abcd' : '2'}, consistency='STRONG')
        tokenmaps = db.get_tokenmaps(self.data_class)
        assert tokenmaps == {'encode': {'test_collection': {'abcdefg': '0', 'ab|cd': '1', 'abcd': '2' }},
                             'decode': {'test_collection': {'0': 'abcdefg', '1': 'ab|cd', '2': 'abcd'}}}
    
    def test_get_config(self):
        config = db.get_config(self.data_class)
        dict_intersection = { key : config[key] for key in set(config) & set(self.config) if self.config[key] == config[key] }
        assert dict_intersection == self.config