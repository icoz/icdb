# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

from icdb.storage.storage import Storage
from icdb.memcache.cache import Cache
from icdb.memcache.hashcache import HashCache
from math import sin, pi
from random import randint

COUNT = 1000

c = Cache()
hc = HashCache()
for i in range(COUNT):
    x = str(i / 180 * pi)
    y = sin(float(x))
    c.set(x, y)
    hc.set(x, y)

for i in range(30):
	key = str(randint(0,COUNT) / 180 * pi)
	vc = float(c.get(key))
	vhc = float(hc.get(key))
	print(c.get(key), hc.get(key), vhc-vc)

