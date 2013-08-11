# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

'''

'''

from storage import Storage
from datetime import datetime, timedelta
import os


class Cache(object):

    """Cache is simple mem-storage for key-value, based on dict()"""

    def __init__(self, fname=None, ttl_count_limit=None):
        super(Cache, self).__init__()
        self.data = dict()
        self.ttl = dict()
        self.filename = fname
        self.limit = ttl_count_limit
        if fname is not None:
            if os.path.exists(fname):
                self.load()

    def get(self, key):
        if type(key) is not str:
            key = str(key)
        try:
            val = self.data[key]
        except KeyError:
            return None
        try:
            ttl = self.ttl[key]
        except KeyError:
            pass
        else:
            print('get: has ttl')
            if type(ttl) is datetime:
                if ttl < datetime.utcnow():
                    print('get: ttl expired')
                    val = None
                    del self.data[key]
                    del self.ttl[key]
        return val

    def set(self, key, value, ttl=None):
        if type(key) is not str:
            key = str(key)
        self.data[key] = value
        if ttl is None:
            try:
                del self.ttl[key]
            except KeyError:
                pass
        if ttl is not None:
            if self.limit is not None:
                if len(self.ttl) > self.limit:
                    self.cleanup()
            if type(ttl) is timedelta:
                self.ttl[key] = datetime.utcnow() + ttl

    def delete(self, key):
        if type(key) is not str:
            key = str(key)
        try:
            # val = self.data[key]
            del self.data[key]
            del self.ttl[key]
        except KeyError:
            pass

    def save(self, fname=None):
        fn = self.filename
        if fname is not None:
            fn = fname
        if fn is not None:
            with Storage(fn) as s:
                for k in self.data:
                    if k not in self.ttl:
                        s.set_unsafe(k, self.data[k])

    def load(self, fname=None):
        fn = self.filename
        if fname is not None:
            fn = fname
        if fn is not None:
            with Storage(fn) as s:
                self.data = s.get_dict()

    def cleanup(self):
        key_to_del = []
        for k in self.ttl:
            v = self.ttl[k]
            if type(v) is datetime:
                if v < datetime.utcnow():
                    del self.data[k]
                    key_to_del.append(k)
        for k in key_to_del:
            del self.ttl[k]
