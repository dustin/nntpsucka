#!/usr/bin/env python
#
# Copyright (c) 2002  Dustin Sallings <dustin@spy.net>
#
# arch-tag: 8F42F3DA-10C0-11D9-A55D-000393CFE6B8

import nntplib
import time
import anydbm
import signal
import os
import sys
import re
import ConfigParser
import logging
import logging.config

# My pidlock
import pidlock

class Stats:
    """Keep statistics describing what got moved."""

    def __init__(self):
        self.moved=0
        self.dup=0
        self.other=0

    def addMoved(self):
        """Mark one article copied."""
        self.moved+=1

    def addDup(self):
        """Mark one article duplicate (not copied)."""
        self.dup+=1

    def addOther(self):
        """Mark one article not copied, but not duplicate."""
        self.other+=1

    def __str__(self):
        return "Moved:  " + str(self.moved) \
            + ", duplicate:  " + str(self.dup) \
            + ", Other:  " + str(self.other)

######################################################################

class NewsDB:
    """Database of seen articles and groups.

    Entries for groups will have keys beginning with ``l/'' and entries for
    individual articles will have keys beginning with ``a/''

    Article entries (a) will have a string value that's a floating point number
    indicating the time that this article was copied.

    Group entries (l) have a string value that represents that last seen
    article by number for the given group (the part after the l/).
    """
    def __init__(self, dbpath):
        self.db=anydbm.open(dbpath, "c")
        self.log=logging.getLogger("NewsDB")

    def hasArticle(self, message_id):
        """Return true if there is a reference to the given message ID in
        this database.
        """
        return self.db.has_key("a/" + message_id)

    def markArticle(self, message_id):
        """Mark the article seen."""
        self.db["a/" + message_id] = str(time.time())

    def getLastId(self, group):
        """Get the last seen article ID for the given group, or 0 if this
        group hasn't been seen.
        """
        rv=0
        try:
            rv=self.db["l/" + group]
        except KeyError:
            pass
        return rv

    def setLastId(self, group, id):
        """Set the last seen article ID for the given group."""
        self.db["l/" + group]=str(id)

    def __del__(self):
        """Close the DB on destruct."""
        self.db.close()

    def getGroupRange(self, group, first, last, maxArticles=0):
        """Get the group range for the given group.

        The arguments represent the group you're looking to copy, and the
        first and last article numbers as provided by the news server.

        The first, last, and count that should be checked will be returned
        as a tuple."""

        # Start with myfirst being one greater than the last thing we've seen
        myfirst=int(self.getLastId(group))+1
        first=int(first)
        last=int(last)
        if (myfirst < first) or (myfirst > last):
            myfirst=first
        mycount=(last-myfirst)+1

        self.log.debug("Want no more than %d articles, found %d from %d\n"
            % (maxArticles, mycount, myfirst))

        if maxArticles > 0 and mycount > maxArticles:
            self.log.debug("Want %d articles with a max of %d...shrinking\n" \
                % (mycount, maxArticles))
            myfirst = myfirst + (mycount - maxArticles)
            mycount = maxArticles
            self.log.debug("New count is %d, starting with %s"
                % (mycount, myfirst))

        return myfirst, last, mycount

######################################################################

class NNTPClient(nntplib.NNTP):
    """An extension of nntplib.NNTP suitable for...well, it actually
    works."""

    headers=['From', 'Subject', 'Message-Id', 'Sender', 'MIME-Version', \
        'Path', 'Newsgroups', 'Organization', 'Approved', 'Sender', \
        'Distribution', \
        'Lines', 'Content-Type', 'Content-Transfer-Encoding']

    def __init__(self, host, port=119,user=None,password=None,readermode=None):
        """See netlib.NNTP"""
        self.log=logging.getLogger("NNTPClient")
        self.log.info("Connecting to %s:%d" % (host, port))
        nntplib.NNTP.__init__(self, host, port, user, password, readermode)
        self.log.debug("Connected to %s:%d" % (host, port))
        self.checkMode()
        self.host=host
        self.port=port

    def __repr__(self):
        return ("<NNTPClient: " + self.host + ":" + `self.port` + ">")

    def checkMode(self):
        """Upon construct, this tries to figure out whether the server
        considers us a feeder or a reader."""
        try:
            self.group('control')
            self.currentmode='reader'
        except nntplib.NNTPPermanentError:
            self.currentmode='poster'
        self.log.debug("Detected mode %s" % (self.currentmode))

    def __headerMatches(self, h):
        """Internal, checks to see if the header ``h'' is in our list of
        approved headers."""
        rv=None
        for header in self.headers:
            if h.lower().find(header.lower()) == 0:
                rv=1
        return rv

    def ihave(self, id):
        """Send an IHAVE for a message ID (feeder mode only).
        
        An exception will be raised if something breaks, or the article
        isn't wanted.  If an exception is not raised, follow with a stream
        of the article."""
        self.log.debug("IHAVing " + id)
        resp = self.shortcmd('IHAVE ' + id)
        self.log.debug("IHAVE returned " + str(resp))

    def copyArticle(self, src, which, messid):
        """Copy an article from the src server to this server.

        which is the ID of the message on the src server, messid is the
        message ID of the article."""
        self.log.debug("Moving " + str(which))
        if self.currentmode == 'reader':
            resp, nr, id, lines = src.article(str(which))
            self.post(lines)
        else:
            self.ihave(messid)
            try:
                resp, nr, id, lines = src.article(str(which))
            except nntplib.NNTPTemporaryError, e:
                # Generate an error, I don't HAVE this article, after all
                self.shortcmd('.')
            self.takeThis(messid, lines)

    def takeThis(self, messid, lines):
        """Stream an article to this server."""
        self.log.debug("*** TAKE THIS! ***")
        for l in lines:
            if l == '.':
                self.log.debug("*** L was ., adding a dot. ***")
                l = '..'
            self.putline(l)
        self.putline('.')
        self.getresp()

    def post(self, lines):
        """Post an article to this server."""
        self.log.debug("*** POSTING! ***")
        resp = self.shortcmd('POST')
        if resp[0] != '3':
            raise nntplib.NNTPReplyError(resp)
        headers=1
        for l in lines:
            if l == '':
                headers=None
                self.putline('')
            else:
                if headers:
                    if self.__headerMatches(l):
                        self.putline(l)
                else:
                    if l == '.':
                        self.log.debug("*** L was ., adding a dot. ***")
                        l = '..'
                    self.putline(l)
        self.putline('.')
        self.getresp()

######################################################################

class NNTPSucka:
    """Copy articles from one NNTP server to another."""

    def __init__(self, src, dest, config=None):
        """Get an NNTPSucka with two NNTPClient objects representing the
        source and destination."""
        self.log=logging.getLogger("NNTPSucka")
        self.src=src
        self.dest=dest
        self.maxArticles=0
        if config is not None:
            try:
                self.maxArticles=config.getint("misc", "maxArticles")
            except ConfigParser.NoSectionError:
                self.log.debug("No section `misc'")
            except ConfigParser.NoOptionError:
                self.log.debug("No option `maxArticles' in section `misc'")
        self.log.debug("Max articles is configured as %d" %(self.maxArticles))
        self.db=NewsDB(config.get("misc","newsdb"))
        self.stats=Stats()

    def copyGroup(self, groupname):
        """Copy the given group from the source server to the destination
        server.
        
        Efforts are made to ensure only articles that haven't been seen are
        copied."""
        self.log.debug("Getting group " + groupname + " from " + `self.src`)
        resp, count, first, last, name = self.src.group(groupname)
        self.log.debug("Done getting group")

        # Figure out where we are
        myfirst, mylast, mycount= self.db.getGroupRange(groupname, first, last,
            self.maxArticles)
        l=[]
        if mycount > 0:
            self.log.info("Copying " + `mycount` + " articles:  " \
                + `myfirst` + "-" + `mylast` + " in " + groupname)

            # Grab the IDs
            resp, l = self.src.xhdr('message-id', `myfirst` + "-" + `mylast`)

            # Validate we got as many results as we expected.
            if(len(l) != mycount):
                self.log.warn("Unexpected number of articles returned.  " \
                    + "Expected " + `mycount` + ", but got " + `len(l)`)

        # Flip through the stuff we actually want to process.
        for i in l:
            try:
                messid="*empty*"
                messid=i[1]
                idx=i[0]
                self.log.debug("idx is " + idx + " range is " + `myfirst` \
                    + "-" + `mylast`)
                assert(int(idx) >= myfirst and int(idx) <= mylast)
                if self.db.hasArticle(messid):
                    self.log.info("Already seen " + messid)
                    self.stats.addDup()
                else:
                    self.dest.copyArticle(self.src, idx, messid)
                    self.db.markArticle(messid)
                    self.stats.addMoved()
                # Mark this message as having been read in the group
                self.db.setLastId(groupname, idx)
            except KeyError, e:
                # Couldn't find the header, article probably doesn't
                # exist anymore.
                pass
            except nntplib.NNTPTemporaryError, e:
                # Save it if it's duplicate
                if str(e).find("Duplicate"):
                    self.db.markArticle(messid)
                    self.stats.addDup()
                else:
                    self.stats.addOther()
                self.log.warn("Failed:  " + str(e))

    def shouldProcess(self, group, ignorelist):
        rv = True
        for i in ignorelist:
            if i.match(group) is not None:
                rv = False
        return rv

    def copyServer(self, ignorelist=[]):
        """Copy all groups that appear on the destination server to the
        destination server from the source server."""
        self.log.debug("Getting list of groups from " + `self.dest`)
        resp, list = self.dest.list()
        self.log.debug("Done getting list of groups from destination")
        for l in list:
            group=l[0]
            if self.shouldProcess(group, ignorelist):
                try:
                    self.log.debug("copying " + `group`)
                    self.copyGroup(group)
                except nntplib.NNTPTemporaryError, e:
                    self.log.warn("Error on group " + group + ":  " + str(e))

    def getStats(self):
        """Get the statistics object."""
        return self.stats

class OptConf(ConfigParser.ConfigParser):
    """ConfigParser with get that supports default values"""

    def __init__(self, defaults=None):
        ConfigParser.ConfigParser.__init__(self, defaults)

    def getWithDefault(self, section, option, default, raw=False, vars=None):
        """returns the configuration entry, or the ``default'' argument"""
        rv=default
        try:
            rv=self.get(section, option, raw, vars)
        except ConfigParser.NoOptionError:
            pass
        return rv

class Timeout:
    """This is an exception that's raised when the alarm handler fires."""
    pass

def alarmHandler(sig, frame):
    """Do nothing but raise a timeout."""
    raise Timeout

def getIgnoreList(fn):
    log=logging.getLogger("nntpsucka")
    log.debug("Getting ignore list from " + fn)
    rv=[]
    f=open(fn)
    for l in f.readlines():
        l=l.strip()
        rv.append(re.compile(l))
    return rv

def main():
    conf=OptConf({'port':'119', 'newsdb':'newsdb', 'pidfile':'nntpsucka.pid'})
    conf.read(sys.argv[1])

    # Lock to make sure only one is running at a time.
    lock=pidlock.PidLock(conf.get("misc", "pidfile"))

    # Let it run up to four hours.
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(4*3600)

    # Validate there's a config file
    if len(sys.argv) < 2:
        sys.stderr.write("Usage:  " + sys.argv[0] + " configFile\n")
        sys.exit(1)

    # Configure logging.
    logging.config.fileConfig(sys.argv[1])

    fromServer=conf.get("servers", "from")
    fromUser=None
    fromPass=None
    fromPort=119
    if conf.has_section(fromServer):
        fromUser=conf.getWithDefault(fromServer, "username", None)
        fromPass=conf.getWithDefault(fromServer, "password", None)
        fromPort=conf.getint(fromServer, "port")
    toServer=conf.get("servers", "to")
    toUser=None
    toPass=None
    toPort=119
    if conf.has_section(toServer):
        toUser=conf.getWithDefault(toServer, "username", None)
        toPass=conf.getWithDefault(toServer, "password", None)
        toPort=conf.getint(toServer, "port")
    filterList=conf.getWithDefault("misc", "filterList", None)

    sucka=None
    # Mark the start time
    start=time.time()
    try:
        s=NNTPClient(fromServer, port=fromPort, user=fromUser, \
            password=fromPass)
        d=NNTPClient(toServer, port=toPort, user=toUser, password=toPass)
        ign=[re.compile('^control\.')]
        if filterList is not None:
            ign=getIgnoreList(filterList)
        sucka=NNTPSucka(s,d, config=conf)
        sucka.copyServer(ign)
    except Timeout:
        sys.stderr.write("Took too long.\n")
    # Mark the stop time
    stop=time.time()

    if sucka:
        # Log the stats
        log=logging.getLogger("nntpsucka")
        log.info(sucka.getStats())
        log.info("Total time spent:  " + str(stop-start) + "s")

if __name__ == '__main__':
    try:
        main()
    except pidlock.AlreadyLockedException, ale:
        sys.stderr.write("Already running:  " + str(ale[0]) + "\n")
