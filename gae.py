import app
from google.appengine.ext.webapp.util import run_wsgi_app

app = app.app
run_wsgi_app(app)
