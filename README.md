heroku-gae-bridge
=================

Deploy same codebase to Heroku or Google App Engine, replicate data between platforms, tokenize properties, etc.

Quick Install (Heroku-y Foreman version)
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

Appengine Setup
===============
```
$ ./gae_ify.sh
$ ~/google_appengine/dev_appserver.py .
```
Verify:
```
$ curl localhost:8080/test/dataclass
data_class.find()
config:
{'DB_NAME': None, 'DB_CLASS_FOLDER': 'gaedatastore_wrapper', 'DB_CONNECTION_STRING': None, 'ENV': 'sandbox', 'DB_CLASS_NAME': 'GaeDatastoreWrapper'}
namespace:
sandbox
results:
[{'_id': 'keyname_1', 'number_property': 10101L, 'string_property': 'yes this is a string'}, {'_id': 'keyname_2', 'number_property': 99L, 'string_property': 'more strings'}]
```

DB Wrapper Tests
================
There are data wrappers for MongoDB, App Engine Datastore, and a generic text database.

Run local tests like so:
```
$ py.test db -v
=========================================================================== test session starts ===========================================================================
platform linux2 -- Python 2.7.3 -- pytest-2.4.2 -- /usr/bin/python
collected 48 items 

test_db.py:16: TestDb.test_db_flatten PASSED
test_db.py:22: TestDb.test_db_unflatten PASSED
...
```

For remote testing against the actual App Engine Datastore:
```
$ py.test db -v --capture=no --env=remote
```
