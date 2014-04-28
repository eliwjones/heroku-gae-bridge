heroku-gae-bridge
=================

Deploy same codebase to Heroku or Google App Engine, replicate data between platforms, tokenize properties, etc.

Quick Install
=============
Prerequisites:
```
$ sudo apt-get install python-setuptools
$ sudo apt-get install python-pip
$ sudo pip install virtualenv
```
Application:
```
$ git clone git@github.com:eliwjones/heroku-gae-bridge.git
$ cd heroku-gae-bridge
$ virtualenv venv --distribute
$ source venv/bin/activate
$ pip install -r requirements.txt
$ foreman start
```
Verify:
```
$ curl localhost:5000
I am a reference application for a pan-cloud application.
$ curl localhost:5000/test/dataclass
data_class.find()
config:
{'DB_CLASS_FOLDER': 'textdb_wrapper', 'DB_NAME': 'app', 'DB_CLASS_NAME': 'TextDbWrapper', 'ENV': 'sandbox', 'DB_CONNECTION_STRING': None}
namespace:
sandbox
results:
[{u'string_property': u'yes this is a string', u'_id': u'keyname_1', u'number_property': 10101}, {u'string_property': u'more strings', u'_id': u'keyname_2', u'number_property': 99}]
```
