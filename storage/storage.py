# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

'''
File contains records (variable size) one by one.
Inspired by Haystack

While saving bytes(bytes.decode) is used by default.

Record:
magic number, 8 bytes
hash(md5), 16 bytes
flags, 2 bytes (so big for 8 byte alignment), 0 - ok, 1 - deleted
key_size, 2 bytes
value_size, 4 bytes
key, <key_size> bytes
value, <value_size> bytes
'''

from hashlib import md5
import struct
from os import rename, remove


def hash_md5(info):
    ''' for hashing using MD5 '''
    m = md5()
    m.update(info.encode())
    return m.digest()


class Storage(object):

    ''' Class Storage for saving key-value pairs using hash '''
    MAGIC_NUMBER = b'\x59\x0d\x1f\x70\xf9\x52\x55\xad'

    def __init__(self, fname):
        ''' init Storage using filename for save data '''
        # print('icdb storage init')
        self.filename = fname
        self.fout = open(fname, 'ab')
        pass

    def __del__(self):
        ''' safely close files before die '''
        # print('icdb storage del')
        self.fout.close()

    def __enter__(self):
        ''' for use with with-statement '''
        # print('icdb storage enter')
        with open(self.filename, 'rb') as fin:
            self.binary_cache = fin.read()  # read all file
        return self

    def __exit__(self, type, value, traceback):
        ''' for use with with-statement, flushes file '''
        # print('icdb storage exit')
        self.fout.flush()
        pass

    def set_unsafe(self, key, value):
        ''' append data in storage. NOTE! if there is key, may be duplicates '''
        if type(key) is not str:
            key = str(key)
        if type(value) is not str:
            value = str(value)
        self.__set_by_hash__(hash_md5(key), key, value)
        pass

    def set(self, key, value):
        ''' create or update data in storage '''
        self.__delete_by_hash__(hash)
        # TODO tests for time used. To run compress
        self.set_unsafe(key, value)
        pass

    def records(self):
        ''' internal. Generator for records '''
        if 'binary_cache' not in self.__dict__:
            with open(self.filename, 'rb') as fin:
                self.binary_cache = fin.read()  # read all file
        b = self.binary_cache
        if len(b) > 32:
            pos = 0
            while True:
                pos = b.find(self.MAGIC_NUMBER, pos)
                if pos == -1:
                    break
                hash = b[pos + 8:pos + 24]
                flags, key_size, val_size = struct.unpack_from(
                    'hhi', b, pos + 24)
                if flags == 0:
                    yield (pos, hash, flags, key_size, val_size)
                pos = pos + 1

    def get_tuple(self):
        ''' get all pairs from storage '''
        arr = []
        with open(self.filename, 'rb') as fin:
            b = fin.read()
            for (pos, hs, flags, key_size, val_size) in self.records():
                key = b[pos + 24 + 2 + 2 + 4:
                        pos + 24 + 2 + 2 + 4 + key_size]
                value = b[pos + 24 + 2 + 2 + 4 + key_size:
                          pos + 24 + 2 + 2 + 4 + key_size + val_size]
                arr.append((key, value))
        return arr

    def get_dict(self):
        ''' get all pairs from storage '''
        arr = dict()
        with open(self.filename, 'rb') as fin:
            b = fin.read()
            for (pos, hs, flags, key_size, val_size) in self.records():
                key = b[pos + 24 + 2 + 2 + 4:
                        pos + 24 + 2 + 2 + 4 + key_size]
                value = b[pos + 24 + 2 + 2 + 4 + key_size:
                          pos + 24 + 2 + 2 + 4 + key_size + val_size]
                # arr.append((key, value))
                arr[key] = value
        return arr

    def compress(self):
        ''' recreates db-file '''
        self.fout.close()
        rename(self.filename, self.filename + '.old')
        with open(self.filename + '.old', 'rb') as fin:
            b = fin.read()
        self.fout = open(self.filename, 'ab')
        pos = 0
        while True:
            pos = b.find(self.MAGIC_NUMBER, pos)
            if pos == -1:
                break
                hash = b[pos + 8:pos + 24]
                flags, key_size, val_size = struct.unpack_from(
                    'hhi', b, pos + 24)
                self.fout.write(b[pos:pos + 32 + key_size + val_size])
            pos = pos + 1
        remove(self.filename + '.old')
        with open(self.filename, 'rb') as fin:
            self.binary_cache = fin.read()

    def get(self, key):
        ''' returns value by given key. Or None if does not exists '''
        if type(key) is not str:
            key = str(key)
        h = hash_md5(key)
        return self.get_by_hash(h)

    def get_by_hash(self, hash):
        ''' returns value by hash. On fail returns None '''
        value = None
        for (pos, hs, flags, key_size, val_size) in self.records():
            # print("get.record_offset= %i hash = %16s, flags = %s" % (pos, hs,
            # flags))
            if hs == hash:
            # if hs == hash and flags == 0:
                # parse record
                # print("get.record_offset= %i hash = %16s, flags = %s, ks=%i,
                # vs=%i" % (pos, hs, flags, key_size, val_size))
                # with open(self.filename, 'rb') as fin:
                #     b = fin.read()
                b = self.binary_cache
                value = b[pos + 24 + 2 + 2 + 4 + key_size:
                          pos + 24 + 2 + 2 + 4 + key_size + val_size]
        return value
                # return value
        # return None

    def __set_by_hash__(self, hash, key, value):
        ''' internal. append record. If one exists - mark it deleted '''
        self.fout.write(self.MAGIC_NUMBER)
        self.fout.write(hash)
        # flags = 0
        s = struct.pack('hhi', 0, len(key), len(value))
        self.fout.write(s)
        self.fout.write(key.encode())
        self.fout.write(value.encode())
        # self.fout.flush()
        pass

    def delete(self, key):
        ''' delete pair by key '''
        self.__delete_by_hash__(hash_md5(key))
        pass

    def __delete_by_hash__(self, hash):
        ''' internal. delete pair by hash '''
        for (pos, hs, *rest) in self.records():
            if hs == hash:
                # set flag deleted
                with open(self.filename, 'r+b') as f:
                    f.seek(pos + 24)
                    f.write(b'\x01')
        pass
