#!/usr/bin/env python
#
# arch-tag: E543AADA-12B1-11D9-89EB-000A957659CC

from __future__ import generators

import getopt
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

def usage():
    print "Usage:  %s [-ag]" % sys.argv[0]
    print " -a dumps articles"
    print " -g dumps groups"
    print " Given no option, both articles and groups will be dumped"
    sys.exit(1)

opts, args=getopt.getopt(sys.argv[1:], 'ag')

copyGroup=True
copyArticle=True

if len(opts) > 0:
    copyGroup=False
    copyArticle=False
    for opt in opts:
        if opt[0] == '-a':
            copyArticle=True
        elif opt[0] == '-g':
            copyGroup=True
        else:
            usage()

if len(args) != 1:
    usage()

db=anydbm.open(args[0])

for k,v in walkDbm(db):
    copyLine=False
    # Figure out whether we want to copy this kind of stuff
    if k[0] == 'l' and copyGroup:
        copyLine=True
    elif k[0] == 'a' and copyArticle:
        copyLine=True

    if copyLine:
        print k + "\t" + v
