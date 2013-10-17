from flask import Blueprint, g, render_template, request
tests = Blueprint('tests', __name__)

@tests.route('/tests')
def render():
    output = []
    # Test put, get, find, update, remove
    base_document1 = {'_id' : 'my_test_id1', 'string_property' : 'string_value', 'integer_property' : 10, 'nested' : {'thing' : 'in_nest'}}
    from copy import deepcopy
    props = deepcopy(base_document1)

    result = g.data_class.put('test_collection', props)

    result = g.data_class.get('test_collection', props)
    output.append("GET: %s" % (result == base_document1))

    result = list(g.data_class.find('test_collection', {'_id' : 'my_test_id1'}))
    output.append("FIND: %s" %  (result == [base_document1]))

    base_document2 = deepcopy(props)
    base_document2['_id'] = 'my_test_id2'
    props2 = deepcopy(base_document2)

    g.data_class.put('test_collection', props2)
    result = list(g.data_class.find('test_collection', {'string_property' : 'string_value'}))
    result.sort()
    comp_list = [base_document1, base_document2]
    comp_list.sort()
    output.append("FIND2: %s" % (result == comp_list))

    if request.args.get('no_remove') is None:
        result = g.data_class.remove('test_collection', {'_id' : 'my_test_id1'})
    result = list(g.data_class.find('test_collection',{}))
    output.append("Remove Result: %s" % result)
    output.append("REMOVE: %s" % (result == [base_document2]))
    if request.args.get('no_remove') is None:
        g.data_class.remove('test_collection', {})

    return str(output)
