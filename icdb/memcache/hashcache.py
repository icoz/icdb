# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

from hashlib import md5


def hash_md5(info):
    ''' for hashing using MD5 '''
    m = md5()
    m.update(str(info).encode())
    return m.digest()


class HashCache(object):

    """
    HashCache is simple mem-storage for key-value
    Implements hash tables for fast search, o(ln n)
    Appending is slow
    """

    def __init__(self):
        '''
        internal:
        hashtable (ht) is list of tuples(hash, key, value)
        ht_count - size of list
        '''
        self.ht = list()
        self.ht_count = 0

    def get(self, key):
        if type(key) is not str:
            key = str(key)
        hash = hash_md5(key)
        try:
            val = self.__get__(hash)[2]
        except KeyError:
            return None
        return val

    def set(self, key, value):
        if type(key) is not str:
            key = str(key)
        # create or update?
        hash = hash_md5(key)
        self.__set__(hash, key, value)

    def delete(self, key):
        if type(key) is not str:
            key = str(key)
        hash = hash_md5(key)
        try:
            self.__delete__(hash, key)
        except KeyError:
            pass

    def __set__(self, hash, key, value):
        '''
        Only appends k-v pair
        If update needed then search and delete key outside
        append - o(n)
        sorted - o(?)
        '''
        # append record to list
        self.ht.append((hash, key, value))
        self.ht_count = self.ht_count + 1
        # sort it
        self.ht = sorted(self.ht)
        pass

    def __get__(self, hash):
        def search(begin, end):
            # print('hs=%s, he=%s, h=%s' % (self.ht[begin][0], self.ht[end][0], hash))
            if end - begin < 2:
                if self.ht[begin][0] == hash:
                    return self.ht[begin]
                if self.ht[end][0] == hash:
                    return self.ht[end]
                raise KeyError
            idx = int(begin + (end - begin) / 2)
            # print('b=%s, idx=%s, e=%s'%(begin,idx,end))
            if self.ht[idx][0] == hash:
                return self.ht[idx]
            if hash < self.ht[idx][0]:
                return search(begin, idx)
            else:
                return search(idx, end)
        # get first
        if hash < self.ht[0][0]:
            raise KeyError
        # get last
        if hash > self.ht[self.ht_count - 1][0]:
            raise KeyError
        # search and return
        return search(0, self.ht_count - 1)
        pass

    def __delete__(self, hash):
        tp = self.__get__(hash)
        self.ht.remove(tp)
        self.ht_count = self.ht_count - 1
        pass
