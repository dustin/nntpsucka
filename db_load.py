#!/usr/bin/env python
#
# arch-tag: 2F822BAE-12B2-11D9-A631-000A957659CC

import anydbm
import string
import sys

db=anydbm.open(sys.argv[1], 'c')

l=sys.stdin.readline()
while l != '':
    l=l.strip()
    a=l.split('\t')
    db[a[0]]=a[1]
    l=sys.stdin.readline()
