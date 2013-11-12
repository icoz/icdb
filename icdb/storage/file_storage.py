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
import struct
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
        self.build_index()

    def __del__(self):
        self.value_file.close()
        self.key_file.close()

    def set(self, key, value):
        # write value-file
        self.value_file.seek(0, SEEK_END)
        value_offset = self.value_file.tell() / 256
        align = self.value_file.tell() - int(value_offset) * 256
        value_offset = int(value_offset)
        # if file suddenly was corrupted then fill till 256 bytes
        for i in range(align):
            self.value_file.write(b'\x00')
        self.value_file.write(value.encode())
        align = 256 - len(value) % 256
        # if len(value) % 256 != 0, then fill end
        for i in range(align):
            self.value_file.write(b'\x00')
        # write key-file
        self.key_file.seek(0, SEEK_END)
        key_offset = self.key_file.tell()
        self.key_file.write(self.KEY_MAGIC_NUMBER)
        key_struct = struct.pack('iiii', 0, len(key), len(value), value_offset)
        self.key_file.write(key_struct)
        self.key_file.write(key.encode())
        # update index
        # if we have such key, then del it!
        index_info = self.index.get(key)
        if index_info is not None:
            i_value_len, i_value_offset, i_key_offset = struct.unpack("iii", index_info)
            self.index.delete(key)
            self.key_file.seek(key_offset + 12, SEEK_SET)
            self.key_file.write(1)
        self.index.set(key, struct.pack('iii', len(value), value_offset, key_offset))
        #print(self.index.ht)
        pass

    def get(self, key):
        # find in index
        index_info = self.index.get(key)
        # if found return data
        if index_info is not None:
            value_len, value_offset, key_offset = struct.unpack("iii", index_info)
            with open(self.filename + '.value', 'rb') as vfile:
                vfile.seek(value_offset * 256)
                value = vfile.read(value_len)
            return value.decode()
        # if not found return None
        else:
            return None

    def delete(self, key):
        # find in index
        k_s = self.index.get(key)
        # find record in file
        if k_s is not None:
            value_len, value_offset, key_offset = struct.unpack("iii", k_s)
            # update flag
            self.key_file.seek(key_offset + 12, SEEK_SET)
            self.key_file.write(1)
        pass

    def compress(self):
        # create new value and key files

        # in loop:
        #   read old key
        #   if not deleted save old value to new value
        pass

    def build_index(self):
        # delete current index
        self.index = HashCache()
        # read .key-file and build index
        for flags, key_size, value_offset, value_size, key, key_offset in self.__keys__():
            print(flags, key_size, value_offset, value_size, key, key_offset)
            self.index.set(key, struct.pack('iii', value_size, value_offset, key_offset))
        pass
        print(self.index.ht)

    def save_index(self):
        pass

    def fix_key_file(self):
        pass

    def __keys__(self):
        """ internal. Generator for records """
        with open(self.filename+'.key', 'rb') as fin:
            b = fin.read()  # read all file
        if len(b) > 32:
            pos = 0
            while True:
                pos = b.find(self.KEY_MAGIC_NUMBER, pos)
                if pos == -1:
                    break
                flags, key_size, value_offset, value_size = struct.unpack_from('iiii', b, pos + 8)
                key = b[pos + 24:pos + 24 + key_size].decode()
                key_offset = pos + 24
                if flags == 0:
                    yield (flags, key_size, value_offset, value_size, key, key_offset)
                pos += 1
