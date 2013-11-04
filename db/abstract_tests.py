
class AbstractTest:
    @classmethod
    def setup_class(self):
        self.config = {'DB_CONNECTION_STRING' : None, 'DB_NAME' : 'testdb', 'ENV' : 'testenv'}
        self.data_class = self.data_wrapper(config = self.config)
        self.data_class.drop_db(self.config['DB_NAME'])

    @classmethod
    def teardown_class(self):
        self.data_class.drop_db(self.config['DB_NAME'])

    def test_put_get(self):
        assert None == self.data_class.get('test_put_get', {'_id' : 'nonexistentid'})
        assert None == self.data_class.put('test_put_get', None)
        result = self.data_class.put('test_put_get', {'_id' : 'keyname0', 'prop' : 'value'})
        assert 'keyname0' == result
        assert self.data_class.get('test_put_get', {'_id' : 'keyname0'}) == {'_id' : 'keyname0', 'prop' : 'value'}

    def test_nested_put_get(self):
        result = self.data_class.put('test_nested_put_get', {'_id' : 'keyname0', 'nested' : {'value' : 'i am nested'}})
        assert 'keyname0' == result
        assert self.data_class.get('test_nested_put_get', {'_id' : 'keyname0'}) == {'_id' : 'keyname0', 'nested' : {'value' : 'i am nested'}}

    def test_nested_get_by_properties(self):
        self.data_class.put('test_nested_get', {'_id' : 'keyname0', 'nested' : {'value' : 'i am nested'}})
        assert self.data_class.get('test_nested_get', {'nested' : {'value' : 'i am nested'}}) == {'_id' : 'keyname0', 'nested' : {'value' : 'i am nested'}}

    def test_find(self):
        for document in [{'_id' : 'keyname0', 'value' : 'one value'}, {'_id' : 'keyname1', 'value' : 'another value'}]:
            self.data_class.put('test_find', document)
        result = list(self.data_class.find('test_find', {}))
        assert result == [{'_id' : 'keyname0', 'value' : 'one value'}, {'_id' : 'keyname1', 'value' : 'another value'}]

    def test_nested_find(self):
        for document in [{'_id' : 'keyname0', 'value' : 'one value', 'nested' : {'value' : 'i am nested'}},
                         {'_id' : 'keyname1', 'value' : 'another value', 'nested' : {'value' : 'i am nested'}}]:
            self.data_class.put('test_nested_find', document)
        assert list(self.data_class.find('test_nested_find', {'nested' : {'value' : 'i am nested'}})) == [{'_id' : 'keyname0', 'value' : 'one value', 'nested' : {'value' : 'i am nested'}},
                                                                                                          {'_id' : 'keyname1', 'value' : 'another value', 'nested' : {'value' : 'i am nested'}}]
    def test_update(self):
        self.data_class.put('test_update', {'_id' : 'keyname0', 'prop1' : 'value one', 'prop2' : 'value two'})
        self.data_class.update('test_update', 'keyname0', {'a new value' : 'with new information'})
        assert {'_id' : 'keyname0', 'prop1' : 'value one', 'prop2' : 'value two', 'a new value' : 'with new information'} == self.data_class.get('test_update', {'_id' : 'keyname0'})
        self.data_class.update('test_update', 'keyname0', {'prop1' : 'new value one'})
        assert {'_id' : 'keyname0', 'prop1' : 'new value one', 'prop2' : 'value two', 'a new value' : 'with new information'} == self.data_class.get('test_update', {'_id' : 'keyname0'})

    def test_remove(self):
        result = self.data_class.put('test_remove', {'_id' : 'keyname0'})
        assert result == 'keyname0'
        self.data_class.remove('test_remove', {'_id' : 'keyname0'})
        assert None == self.data_class.get('test_remove', {'_id' : 'keyname0'})
