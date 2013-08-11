
if __name__ == "__main__" and __package__ is None:
    print(__name__, __package__)
    __package__ = "icdb.tests"
    print(__name__, __package__)

from icdb.memcache import CacheTTL
from math import sin
from datetime import timedelta
from time import sleep

c = CacheTTL(limit=8000)
COUNT = 10000
