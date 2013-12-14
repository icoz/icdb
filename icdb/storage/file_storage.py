# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

"""
Struct of FileStorage
---------------------
FileStorage keeps data in 3 types of files: .idx, .key, .value
.idx - index for keeping hash, key-offset, value-offset
.key - file to save keys (binary)
.value - file to save values (binary)

Limits
------
Key size is max 2^32 bytes
Value size is max 2^40 bytes

.idx struct
-----------
Header:
  magic, 4 byte = 0x720AE06A, b'\x6a\xe0\x0a\x72'
  version id, 4 byte = 0x00000001
  records count, 4 byte
  reserved, 4 byte

  records, (records count)*sizeof(Record), sorted by hash
Record:
  #hash, 16 bytes, md5(key)
  key_offset, 4 bytes
  key_size, 4 bytes = count of bytes
  value_offset, 4 bytes = count of blocks(256-bytes) to skip
  value_size, 4 bytes = count of 256-bytes

.key struct
-----------
Contents records of variable size
Record:
  magic, 8 byte = b'\x50\x0a\x6f\x70\xf2\x52\x55\xad'
  key_size, 4 bytes = count of bytes
  flags, 4 byte = 0 - ok, non 0 - deleted
  value_offset, 4 bytes = count of blocks(256-bytes) to skip
  value_size, 4 bytes = count of 256-bytes
  key, bytes[]

.value struct
-------------
Contents records of variable size
Record:
  value, bytes[], aligned to 256 bytes
"""

from hashlib import md5
from io import SEEK_END, SEEK_SET
import os
import struct
from unittest import TestCase
from icdb.memcache.hashcache import HashCache


def hash_md5(info):
    """ for hashing using MD5 """
    m = md5()
    m.update(str(info).encode())
    return m.digest()


class FileStorage(object):
    """
    FileStorage
    """
    KEY_MAGIC_NUMBER = b'\x50\x0a\x6f\x70\xf2\x52\x55\xad'
    IDX_MAGIC_NUMBER = b'\x6a\xe0\x0a\x72'

    def __init__(self, filename='test.icdb'):
        # super(FileStorage, self).__init__()
        self.filename = filename
        self.index = HashCache()
        self.value_file = open(filename + '.value', 'ab')
        self.key_file = open(filename + '.key', 'ab')
        if os.path.exists(self.filename + '.idx'):
            self.load_index()
        else:
            self.build_index()

    def __del__(self):
        self.value_file.close()
        self.key_file.close()
        self.build_index()
        self.save_index()

    def __setitem__(self, key, value):
        value_offset, value_size = self.__save_value_record__(value)
        key_offset = self.__save_key_record__(key, value_offset, value_size)
        # update index
        # if we have such key, then del it!
        i_key_offset, *rest = self.__get_from_index__(key)
        if i_key_offset is not None:
            self.index.delete(key)
            self.key_file.seek(i_key_offset + 12, SEEK_SET)
            self.key_file.write(b'\x01')
        self.__put_to_index__(key, key_offset, value_offset, value_size)

    def __getitem__(self, key):
        # find in index
        key_offset, value_offset, value_size = self.__get_from_index__(key)
        if key_offset is None:
            for flags, key_size, value_offset, value_size, k, key_offset in self.__keys__():
                if k == key:
                    break
                    # if no such key in key-file, then return None
            if k != key:
                return None
        with open(self.filename + '.value', 'rb') as vfile:
            vfile.seek(value_offset * 256)
            value = vfile.read(value_size)
        return value.decode()
        # old
        index_info = self.index[key]
        # if found return data in index
        value_offset = 0
        value_size = 0
        k = key
        if index_info is not None:
            key_offset, value_offset, value_size = struct.unpack("iii", index_info)
        # if not found then full search in
        else:
            for flags, key_size, value_offset, value_size, k, key_offset in self.__keys__():
                if k == key:
                    break
                    # if no such key in key-file, then return None
        if k != key:
            return None
        with open(self.filename + '.value', 'rb') as vfile:
            vfile.seek(value_offset * 256)
            value = vfile.read(value_size)
        return value.decode()

    def delete(self, key):
        # find in index
        k_s = self.index[key]
        # find record in file
        if k_s is not None:
            value_len, value_offset, key_offset = struct.unpack("iii", k_s)
            # update flag
            self.key_file.seek(key_offset + 12, SEEK_SET)
            self.key_file.write(1)
        pass

    def compress(self):
        # create new value and key files
        nv = open(self.filename + ".nvalue", wb)
        nk = open(self.filename + ".nkey", wb)
        # in loop:
        #   read old key
        #   if not deleted save old value to new value
        for flags, key_size, value_offset, value_size, key, key_offset in self.__keys__():
            if flags == 0:
                #   get key record

                #   save key record
                # get value record
                # set value record
                pass
        pass

    def build_index(self):
        """
        Builds new index from key-file
        """
        # delete current index
        self.index = HashCache()
        # read .key-file and build index
        for flags, key_size, value_offset, value_size, key, key_offset in self.__keys__():
            #print(flags, key_size, value_offset, value_size, key, key_offset)
            self.__put_to_index__(key, key_offset, value_offset, value_size)

    def __keys__(self):
        """ internal. Generator for records
        raises: (flags, key_size, value_offset, value_size, key, key_offset)
        """
        with open(self.filename + '.key', 'rb') as fin:
            b = fin.read()  # read all file
        if len(b) > 32:
            pos = 0
            while True:
                pos = b.find(self.KEY_MAGIC_NUMBER, pos)
                if pos == -1:
                    break
                key_size, flags, value_offset, value_size = struct.unpack_from('iiii', b, pos + 8)
                key = b[pos + 24:pos + 24 + key_size].decode()
                key_offset = pos + 24
                if flags == 0:
                    yield (flags, key_size, value_offset, value_size, key, key_offset)
                pos += 1

    def __save_value_record__(self, value):
        """ saves value to file
        return: value_offset and value_size
        """
        #value_type = type(value)
        value = str(value)
        # write value-file
        self.value_file.seek(0, SEEK_END)
        value_offset = self.value_file.tell() / 256
        align = self.value_file.tell() - int(value_offset) * 256
        # if file suddenly was corrupted then fill till 256 bytes
        for i in range(align):
            self.value_file.write(b'\x00')
            #self.value_file.write(value_type)
        value_offset = int(self.value_file.tell() / 256)
        self.value_file.write(value.encode())
        align = 256 - len(value) % 256
        # if len(value) % 256 != 0, then fill end
        for i in range(align):
            self.value_file.write(b'\x00')
        self.value_file.flush()
        return value_offset, len(value)

    def __save_key_record__(self, key, value_offset, value_size):
        """ saves key-record to file
        return: key_offset
        """
        key = str(key)
        # write key-file
        self.key_file.seek(0, SEEK_END)
        key_offset = self.key_file.tell()
        self.key_file.write(self.KEY_MAGIC_NUMBER)
        key_struct = struct.pack('iiii', len(key), 0, value_offset, value_size)
        self.key_file.write(key_struct)
        self.key_file.write(key.encode())
        self.key_file.flush()
        return key_offset

    def __get_key_record__(self, key_offset):
        """
        Returns (key, flags, value_offset, value_size)
        """
        with open(self.filename + '.key', 'rb') as f_key:
            # get file length
            f_len = f_key.seek(0, SEEK_END)
            if f_len < key_offset:
                raise IndexError
                # set pos to key_offset
            f_key.seek(key_offset, SEEK_SET)
            # read record
            magic = f_key.read(8)
            # check record
            if magic == self.KEY_MAGIC_NUMBER:
                k_s = f_key.read(4 * 4)
                key_size, flags, value_offset, value_size = struct.unpack('iiii', k_s)
                key = f_key.read(key_size)
                return (key, flags, value_offset, value_size)
            else:
                # if not valid, then rebuild index
                self.build_index()
                raise IndexError

    def __put_to_index__(self, key, key_offset, value_offset, value_size):
        """
        puts to index (key_offset, value_offset, value_size)-struct for 'key'
        """
        self.index[key] = struct.pack('iii', key_offset, value_offset, value_size)

    def __get_from_index__(self, key):
        """
        returns (key_offset, value_offset, value_size)-struct for 'key' if found
        if not found returns None, None, None
        """
        index_info = self.index[key]
        if index_info is not None:
            key_offset, value_offset, value_size = struct.unpack("iii", index_info)
            return key_offset, value_offset, value_size
        else:
            return None, None, None

    def load_index(self):
        """
        load previously saved index
        """
        with open(self.filename + ".idx", 'rb') as idx:
            magic = idx.read(4)
            if magic != self.IDX_MAGIC_NUMBER:
                raise FileNotFoundError
            ver_id, rec_count, reserved = struct.unpack("iii", idx.read(4 * 3))
            if ver_id != 1:
                raise FileNotFoundError
            for i in range(rec_count):
                k_off, k_size, v_off, v_size = struct.unpack("iiii", idx.read(4 * 4))
                key, *rest = self.__get_key_record__(k_off)
                self.__put_to_index__(key, k_off, v_off, v_size)

    def save_index(self):
        """ Save index to file
        You can now just load index, not rebuild it on start
        """
        with open(self.filename + ".idx", 'wb') as idx:
            # write header
            idx.write(self.IDX_MAGIC_NUMBER)
            idx.write(b'\x01\x00\x00\x00')
            idx.write(struct.pack('i', self.index.ht_count))
            idx.write(b'\x00\x00\x00\x00')
            # write body
            for i in range(self.index.ht_count):
                hash, key, index_info = self.index.ht[i]
                key_offset, value_offset, value_size = struct.unpack('iii', index_info)
                rec = struct.pack('iiii', key_offset, len(key), value_offset, value_size)
                idx.write(rec)

    def fix_key_file(self):
        pass


class FileStorageTest(TestCase):
    def setUp(self):
        try:
            os.unlink('test.icdb.key')
            os.unlink('test.icdb.value')
            os.unlink('test.icdb.idx')
        except FileNotFoundError:
            pass
        self.fs = FileStorage()

    def test_set_get(self):
        self.fs['123'] = 123
        print("value of 123 = ", self.fs['123'])
        self.assertEqual('123', self.fs['123'])
        self.fs['123'] = 'some'
        print("value of 123 = ", self.fs['123'])
        self.assertEqual('some', self.fs['123'])
        self.fs['311'] = "some text"
        self.assertEqual('some text', self.fs['311'])

    def test_load_index(self):
        self.fs['123'] = 123
        self.fs.save_index()
        self.fs['111'] = '111'
        self.fs.load_index()
        try:
            print(self.fs['111'])
        except IndexError:
            return True
        else:
            return False

