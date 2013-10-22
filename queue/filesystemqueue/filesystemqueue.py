"""Indebted to the ever sexy deferred from google.appengine.ext"""

import pickle, random, string, os, types

_QUEUE_DIR = "/home/mrz/github_repos/eliwjones/heroku-gae-bridge/pmq"

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
        queueitem_key = os.listdir(_QUEUE_DIR).pop()
    except:
        print "No work to do!"
        return
    with open("%s/%s" % (_QUEUE_DIR, queueitem_key)) as queueitem:
        func, args, kwds = pickle.load(queueitem)
    try:
        os.remove("%s/%s" % (_QUEUE_DIR, queueitem_key))
    except:
        print "Presumably remove failed since already removed.  So silently exit."
        return
    return func(*args, **kwds)
