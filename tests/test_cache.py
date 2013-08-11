from icdb.memcache.cache_ttl import CacheTTL
from math import sin, pi
from datetime import timedelta, datetime
# from time import sleep
from random import randint

# consts for playing with CacheTTL
COUNT = 10000
LIMIT = 10005


def main():
    c = CacheTTL(limit=LIMIT)
    cache_miss = 0
    cache_hit = 0

    start_time = datetime.utcnow()
    for round in range(1000000):
        x = str(randint(0, COUNT) / 180 * pi)[:10]
        y = c.get(x)
        if y is None:
            # print('cache miss, calculating for x=', x)
            y = sin(float(x))
            c.set(x, y, timedelta(seconds=5))
            cache_miss = cache_miss + 1
        else:
            cache_hit = cache_hit + 1
        if round % 10000 == 0:
            cur_time = datetime.utcnow() - start_time
            start_time = datetime.utcnow()
            print('Round = %i, timedelta = (' % round, cur_time, ')')
            print('  cache fill = %i' % len(c.data))
            print('  cache miss = %i' % cache_miss)
            print('  cache hit = %i' % cache_hit)

if __name__ == '__main__':
    main()
