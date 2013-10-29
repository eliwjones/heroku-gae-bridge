import os
from flask import Flask, Response
    
app = Flask(__name__, static_folder='static')
app.config.from_object('default_settings')
if 'APPENGINE' in os.environ.keys():
    app.config.from_object('appengine_settings')
app.config.from_envvar('ENV_SETTINGS', silent=True)

data_wrapper = __import__('db.' + app.config['DB_CLASS_FOLDER'], fromlist = [app.config['DB_CLASS_NAME']])
_class = getattr(data_wrapper, app.config['DB_CLASS_NAME'])
data_class = _class(app)


@app.route('/')
def index():
    return Response("I am a reference application for a pan-cloud application.", content_type = "text/plain")

@app.route("/replicate/batch", methods=['GET', 'POST'])
def replicate_batch():
    from flask import request
    if request.method == 'GET':
        return "I expect a POST"
    data_class.accept_replicated_batch(request.data)
    return "Thanks"

@app.route('/test/dataclass')
def test_dataclass():
    collection = 'my_test_collection'
    results = list(data_class.find(collection, {}))
    if not results:
        data_class.put(collection, {'_id' : 'keyname_1', 'string_property' : 'yes this is a string', 'number_property' : 10101})
        data_class.put(collection, {'_id' : 'keyname_2', 'string_property' : 'more strings', 'number_property' : 99})
        results = list(data_class.find(collection, {}))
    return Response("data_class.find()\nconfig:\n%s\nnamespace:\n%s\nresults:\n%s" % (data_class.config, data_class.ns, results), content_type="text/plain")

if 'APPENGINE' not in os.environ.keys():
    @app.route("/pmqtest")
    def test_pmq():
        import time
        current_time = time.asctime()
        collection = 'pmq_test_collection'
        keyname = data_class.update(collection, 'pmq_test_keyname', {'_id' : 'pmq_test_keyname', 'nested' : {'time' : {'info': current_time}}}, upsert=True, replace=True)
        from queue import filesystemqueue
        from queue.consumers import read_textdb_func
        filesystemqueue.defer(read_textdb_func, collection, {'_id':keyname}, app.config)
        return Response("Inserted keyname: %s! Check pmq log!!" % (keyname))
    @app.route("/pmqwork")
    def insert_work():
        from queue import filesystemqueue
        from queue.consumers import my_func
        filesystemqueue.defer(my_func, "hello from heroku", _name = 'myfunctest')
        return Response("Inserted stuff to pmq.", content_type = "text/plain")
    @app.route("/pmqlog")
    def show_log():
        try:
            with open('pmq.log', 'r') as pmqlog:
                logresults = pmqlog.read()
        except:
            logresults = "No pmqlog results"
        return Response("LOG RESULTS:\n%s" % (logresults), content_type="text/plain")


if 'APPENGINE' in os.environ.keys():
    from google.appengine.ext.webapp.util import run_wsgi_app
    run_wsgi_app(app)
if __name__ == '__main__':
    app.run()