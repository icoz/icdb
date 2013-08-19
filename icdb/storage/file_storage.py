# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

'''
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
  # hash, 16 bytes, md5(key)
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
'''

from hashlib import md5
import struct
from os import rename, remove
from icdb.memcache.hashcache import HashCache


def hash_md5(info):
    ''' for hashing using MD5 '''
    m = md5()
    m.update(str(info).encode())
    return m.digest()

class FileStorage(object):
  """
  FileStorage
  """
  def __init__(self, filename='test.icdb'):
    # super(FileStorage, self).__init__()
    self.filename = filename
    self.index = HashCache()
    self.fout_value = open(filename+'.value','ab')

  def set(self,key,value):
    # write value-file
    # write key-file
    # update index
    pass

  def get(self,key):
    # get hash
    # find in index
    # if found return data
    # if not found return None
    pass

  def delete(self,key):
    # find record in file
    # update flag
    pass

  def compress(self):
    pass

  def build_index(self):
    pass

  def update_index(self):
    pass

  def save_index(self):
    pass