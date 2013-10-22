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
    @app.route("/pmqlog")
    def show_log():
        logresults = "No pmqlog results"
        with open('pmq.log', 'r') as pmqlog:
            logresults = pmqlog.read()
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



@app.before_request
def global_data_class():
    g.data_class = data_class


if 'APPENGINE' in os.environ.keys():
    from google.appengine.ext.webapp.util import run_wsgi_app
    run_wsgi_app(app)
if __name__ == '__main__':
    app.run()
