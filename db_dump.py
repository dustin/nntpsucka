#!/usr/bin/env python
#
# arch-tag: E543AADA-12B1-11D9-89EB-000A957659CC

from __future__ import generators

import anydbm
import sys

def walkDbm(dbm):
    finished=None
    k,v=dbm.first()
    while 1:
        yield k,v
        try:
            k,v=dbm.next()
        except KeyError:
            raise StopIteration

db=anydbm.open(sys.argv[1])

for k,v in walkDbm(db):
    print k + "\t" + v
