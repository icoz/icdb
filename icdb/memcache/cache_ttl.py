# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

from icdb.storage.storage import Storage
from datetime import datetime, timedelta
import os


class CacheTTL(object):

    """
    CacheTTL is mem-storage for key-value
    - based on dict()
    - uses ttl for store values, default ttl = 1 min
    - can exceed limit on put key-value, but it will be slow,
        because on every put cleanup() will be called
    - save/load is implemented
    - load will cleanup values with timeouted TTL
    - saving in two files: filename, filename.ttl
    """

    def __init__(self, filename=None, limit=1000):
        '''
        if filename is passed, then try to load from file
        limit by default = 1000
        '''
        self.data = dict()
        self.ttl = dict()
        self.limit = int(limit)
        if filename is not None:
            if os.path.exists(filename):
                self.load(filename)

    def get(self, key):
        '''
        Get value by key
        If value is timeouted 'None' will be returned
        '''
        if type(key) is not str:
            key = str(key)
        # find value for key
        try:
            val = self.data[key]
        except KeyError:
            return None
        # find ttl for key
        try:
            ttl = self.ttl[key]
        except KeyError:
            # if no ttl, but we have value for this key, then del value
            del self.data[key]
            return None
        else:
            if type(ttl) is datetime:
                if ttl < datetime.utcnow():
                    # print('get: ttl expired')
                    val = None
                    del self.data[key]
                    del self.ttl[key]
            else:
                # if ttl is not datetime, then del ttl and value for this key
                val = None
                del self.data[key]
                del self.ttl[key]
        return val

    def set(self, key, value, ttl=timedelta(minutes=1)):
        '''
        Set key-value with given TTL
        If TTL is not type(timedelta) then no value will be stored, None will be returned
        '''
        if type(key) is not str:
            key = str(key)
        if type(ttl) is not timedelta:
            return None
        self.data[key] = value
        self.ttl[key] = datetime.utcnow() + ttl
        if len(self.ttl) > self.limit:
            self.cleanup()

    def delete(self, key):
        '''
        Delete value (and TTL) for given key-value
        '''
        if type(key) is not str:
            key = str(key)
        try:
            del self.data[key]
            del self.ttl[key]
        except KeyError:
            pass

    def cleanup(self):
        ''' Cleanup outdated values '''
        key_to_del = []
        # find keys with timeouted TTL
        for k in self.ttl:
            v = self.ttl[k]
            if type(v) is datetime:
                if v < datetime.utcnow():
                    key_to_del.append(k)
        # delete values and TTLs for it
        for k in key_to_del:
            del self.ttl[k]
            del self.data[k]
        # check values without ttl
        if len(self.data) > len(self.ttl):
            for k in self.data:
                try:
                    self.ttl[k]
                except KeyError:
                    del self.data[k]
        # check ttl without values
        if len(self.data) < len(self.ttl):
            for k in self.ttl:
                try:
                    self.value[k]
                except KeyError:
                    del self.ttl[k]

    def save(self, fname=None):
        '''
        Save cache to file 'fname'
        If fname is None, None will be saved. ;-)
        '''
        if fname is not None:
            with Storage(fname) as s:
                for k in self.data:
                    s.set_unsafe(k, self.data[k])
            with Storage(fname + '.ttl') as s:
                for k in self.data:
                    s.set_unsafe(k, self.data[k])

    def load(self, fname=None, append=False):
        '''
        Load cache from file 'fname'
        Current cache will be removed (by default), to append data from file to cache, set param 'append=True'
        If fname is None, None will be loaded. ;-) But current cache stays alive.
        '''
        if fname is not None:
            with Storage(fname) as s:
                data = s.get_dict()
            with Storage(fname + '.ttl') as s:
                ttl = s.get_dict()
            if append:
                for k in data:
                    self.data[k] = data[k]
                    self.ttl[k] = ttl[k]
            # cleanup loaded key-values for timeouted TTL
            self.cleanup()
