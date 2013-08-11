# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

from icdb.storage.storage import Storage
from datetime import datetime, timedelta
import os


class CacheMW(object):

    """
    CacheMW is mem-storage for key-value, with limit on count of stored key-values
    - based on dict()
    - last time used (LTU) saves with value
    - on set check limit and if exceed delete most old LTU in cache
    """

    def __init__(self, filename=None, limit=1000, on_limit_cleanup=100):
        '''
        if filename is passed, then try to load from file
        limit by default = 1000, limits count of pairs(key,value) in cache
        on_limit_cleanup = 100, how much records we must delete in cache on limit
        '''
        # data is dict(key: tuple(value, ltu), key: tuple(value, ltu) ...)
        self.data = dict()
        self.limit = int(limit)
        self.on_limit_cleanup = int(on_limit_cleanup)
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
            val = self.data[key][0]
            self.data[key][1] = datetime.utcnow()
        except KeyError:
            return None
        return val

    def set(self, key, value):
        '''
        Set key-value
        If exceeds limit, then kill most old LTU
        '''
        if type(key) is not str:
            key = str(key)
        self.data[key] = [value, datetime.utcnow()]
        if len(self.data) > self.limit:
            self.cleanup()

    def delete(self, key):
        '''
        Delete value (and LTU) for given key-value
        '''
        if type(key) is not str:
            key = str(key)
        try:
            del self.data[key]
        except KeyError:
            pass

    def cleanup(self):
        '''
        Cleanup values with most old LTU
        count for delete can be set by defining on_limit_cleanup in __init__
        '''
        # get ltu and keys
        ltu_key = dict()
        ltu_sort = []
        for k in self.data:
            # use 'LTU' as key, 'key' as value
            ltu = self.data[k][1]
            ltu_key[ltu] = k
            ltu_sort.append(ltu)
        # sort ltu
        ltu_sort = sorted(ltu_sort)
        # get top100 and kill 'em
        # we must left in cache only (limit-on_limit_cleanup) values
        # so we must kill 'count' entries
        count = len(self.data) - (self.limit - self.on_limit_cleanup)
        for i in range(count):
            del self.data[ltu_key[ltu_sort[i]]]

    def save(self, fname=None):
        '''
        Save cache to file 'fname'
        If fname is None, None will be saved. ;-)
        '''
        if fname is not None:
            with Storage(fname) as s:
                for k in self.data:
                    s.set_unsafe(k, self.data[k][0])

    def load(self, fname=None, append=False):
        '''
        Load cache from file 'fname'
        Current cache will be removed (by default), to append data from file to cache, set param 'append=True'
        If fname is None, None will be loaded. ;-) But current cache stays alive.
        '''
        if fname is not None:
            with Storage(fname) as s:
                data = s.get_dict()
            # if not append, then delete current data
            if not append:
                del self.data
                self.data = dict()
            for k in data:
                self.data[k] = (data[k], datetime.utcnow())
            # cleanup loaded key-values if exceeds limit
            if len(self.data) > self.limit:
                self.cleanup()
