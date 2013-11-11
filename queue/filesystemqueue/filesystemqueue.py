"""Indebted to the ever sexy deferred from google.appengine.ext"""

import pickle, random, string, os, types, glob

_QUEUE_DIR = os.path.dirname(os.path.abspath(__file__))
if _QUEUE_DIR.endswith('/queue/filesystemqueue'):
    _QUEUE_DIR = _QUEUE_DIR[:-len('/queue/filesystemqueue')]
_QUEUE_DIR += '/pmq'

def defer(obj, *args, **kwargs):
    item_name = kwargs.pop('_name', 'filesystemqueueitem')
    mytuple = (obj, args, kwargs)
    rand_str = ''.join(random.choice(string.lowercase) for i in range(10))
    queueitem_key = "%s_%s" % (item_name, rand_str)
    try:
        with open("%s/%s" % (_QUEUE_DIR, queueitem_key), 'w') as queueitem:
            pickle.dump(mytuple, queueitem)
        return queueitem_key
    except Exception:
        # Do retries? Throw?
        raise Exception

def work():
    # Too annoying to find out how to get oldest file without bullshit contortions.
    try:
        queueitem_path = glob.glob("%s/*" % (_QUEUE_DIR)).pop()
    except:
        return
    with open("%s" % (queueitem_path)) as queueitem:
        func, args, kwds = pickle.load(queueitem)
    try:
        os.remove("%s" % (queueitem_path))
    except:
        print "Presumably remove failed since already removed.  So silently exit."
        return
    return func(*args, **kwds)
