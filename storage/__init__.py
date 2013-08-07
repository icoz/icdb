#!/usr/bin/python

from storage import Storage
from math import sin


def main():
    with Storage('test.icdb') as s:
        # s.set('key1', 'val1')
        # s.set('key2', 'value2')
        # s.set('megakey', 'megavalue')
        # print('main:', s.get('key2'))
        pi = 3.14
        for i in range(10000):
            s.set(str(pi * i / 100), str(sin(pi * i / 100)))
        print('sin(pi*77/100)=', s.get(str(pi * 77 / 100)))

if __name__ == '__main__':
    main()
