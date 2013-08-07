from cache import Cache
from math import sin
from datetime import timedelta
from time import sleep

c = Cache(ttl_count_limit = 8000)
COUNT = 1000000
COUNT_TTL = 100
for i in range(COUNT):
        x = 3.14*i/COUNT
        y = sin(x)
        c.set(x,y)
for i in range(COUNT_TTL):
        x = 3.14*i/COUNT_TTL
        y = sin(x)
        c.set(x,y, timedelta(seconds=4))
print(len(c.data))
print ('sin(%f) = %s' % (3.14*35/COUNT,c.get(3.14*35/COUNT)))
print ('sin(%f) = %s' % (3.14*77/COUNT_TTL,c.get(3.14*77/COUNT_TTL)))
print ('sin(%f) = %s' % (3.14*(COUNT_TTL-10)/COUNT_TTL,c.get(3.14*(COUNT_TTL-10)/COUNT_TTL)))
c.save('sin_cache.icdb')
sleep(5)
print(len(c.data))
c.cleanup()
print(len(c.data))
print ('sin(%f) = %s' % (3.14*35/COUNT,c.get(3.14*35/COUNT)))
print ('sin(%f) = %s' % (3.14*77/COUNT_TTL,c.get(3.14*77/COUNT_TTL)))
print ('sin(%f) = %s' % (3.14*(COUNT_TTL-10)/COUNT_TTL,c.get(3.14*(COUNT_TTL-10)/COUNT_TTL)))
