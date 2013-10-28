import glob, os, sys
from flask import Flask, g

app = Flask(__name__, static_folder='static')
app.config.from_object('default_settings')
app.config.from_envvar('ENV_SETTINGS', silent=True)
if 'APPENGINE' in os.environ.keys():
    app.config['DB_CLASS_FOLDER'] = 'gaedatastore_wrapper'
    app.config['DB_CLASS_NAME'] = 'GaeDatastoreWrapper'
else:
    from flask import Response
    @app.route("/pmqtest")
    def test_pmq():
        import time
        current_time = time.asctime()
        collection = 'pmq_test_collection'
        keyname = g.data_class.update(collection, 'pmq_test_keyname', {'_id' : 'pmq_test_keyname', 'nested' : {'time' : {'info': current_time}}}, upsert=True, replace=True)
        from queue import filesystemqueue
        from queue.consumers import *
        filesystemqueue.defer(read_textdb_func, collection, {'_id':keyname}, app.config)
        return Response("Inserted keyname: %s! Check pmq log!!" % (keyname))
    @app.route("/pmqwork")
    def insert_work():
        from queue import filesystemqueue
        from queue.consumers import *
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

data_wrapper = __import__('db.' + app.config['DB_CLASS_FOLDER'], fromlist = [app.config['DB_CLASS_NAME']])
_class = getattr(data_wrapper, app.config['DB_CLASS_NAME'])
data_class = _class(app)


filepath = os.path.dirname(os.path.abspath(__file__)) + "/views"
views = [os.path.basename(module)[:-3] for module in glob.glob("views/*.py")]
views = [view for view in views if view != '__init__']
for view in views:
    view_module = __import__('views.' + view, fromlist = [view])
    blueprint = getattr(view_module, view)
    app.register_blueprint(blueprint)


@app.route("/replicate/batch", methods=['GET', 'POST'])
def put_batch():
    from flask import request
    if request.method == 'GET':
        return "I expect a POST"
    data_class.accept_replicated_batch(request.data)
    return "Thanks"

@app.before_request
def global_data_class():
    g.data_class = data_class


if 'APPENGINE' in os.environ.keys():
    from google.appengine.ext.webapp.util import run_wsgi_app
    run_wsgi_app(app)
if __name__ == '__main__':
    app.run()
