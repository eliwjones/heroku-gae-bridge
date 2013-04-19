import webapp2
from webapp2_extras import jinja2


class BaseHandler(webapp2.RequestHandler):
    @webapp2.cached_property
    def jinja2(self):
        return jinja2.get_jinja2(app=self.app)
    def render_response(self, _template, **context):
        rv = self.jinja2.render_template(_template, **context)
        self.response.write(rv)

class HelloWebapp2(BaseHandler):
    def get(self):
        context = {'message' : 'Hello, webapp2! from ' + self.request.host}
        self.render_response('index.html', **context)

app = webapp2.WSGIApplication([
    ('/', HelloWebapp2),
], debug=True)
