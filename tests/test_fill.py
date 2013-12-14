#from icdb.storage.storage import Storage
from math import sin, pi

from icdb.memcache.cache import Cache
from icdb.storage.file_storage import FileStorage


# with Storage('sin.icdb') as s:
#      for i in range(100000):
#              x = 3.14*i/100000
#              y = sin(x)
#              s.set_unsafe(str(x),str(y))

COUNT = 1000

c = Cache()
for i in range(COUNT):
    x = str(i / 180 * pi)
    y = sin(float(x))
    c.set(x, y)
c.save('sin.icdb')

c2 = Cache('sin.icdb')

for k in c.data:
    try:
        if float(c.data[k]) != float(c2.get(k)):
            print('Something is wrong! Values differs for key (%s) = (%s, %s)' % (k, c.data[k], c2.data[k]))
    except KeyError:
        print('KeyError! key = %s' % k)
    else:
        c2.delete(k)
if len(c2.data) > 0:
    print('In c2 some data has left...')
    print(c2.data)

fs = FileStorage('my.icdb')
fs["123"] = "123 ok"
fs["2345632"] = "vnesvk fnvksnf v"
fs["sldf,3"] = "kldsnf snfvdnsfk vnsdf"
print(fs["123"])
print(fs["2345632"])

for i in range(COUNT):
    x = str(i / 180 * pi)
    y = sin(float(x))
    fs[x] = y