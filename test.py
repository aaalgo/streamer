#!/usr/bin/env python3
import sys
sys.path.append('build/lib.linux-x86_64-' + sys.version[:3])
import cpp

def generator ():
    for i in range(1000):
        yield i

stream = cpp.Streamer(generator(), 4);

while True:
    v = next(stream)
    if v is None:
        break
    print(v)

