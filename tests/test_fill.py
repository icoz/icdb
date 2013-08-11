from storage import Storage
from math import sin

with Storage('sin.icdb') as s:
     for i in range(100000):
             x = 3.14*i/100000
             y = sin(x)
             s.set_unsafe(str(x),str(y))
