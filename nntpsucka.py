#!/usr/bin/env python
#
# Copyright (c) 2002  Dustin Sallings <dustin@spy.net>
#
# 
from __future__ import division
from sqlite3 import dbapi2 as sqlite
import ConfigParser
import Queue
import datetime
import logging
import logging.config
import nntplib
import os
import re
import signal
import sys
import threading
import time
import traceback
import pidlock
import mailbox
from zipfile import ZipFile

#lock = threading.Lock()

MBOX_DIR="/mnt/ST4T1/usenethist"

print("start imports done")

# Configuration defaults
CONF_DEFAULTS={'port':'4119', 'newsdb':'newsdb', 'pidfile':'nntpsucka.pid',
    'shouldMarkArticles': 'true', 'maxArticles': 0}
CONF_SECTIONS=['misc', 'servers']

GROUPS = list()
CONFIG = dict()
CONFIG['running_workers'] = 0
CONFIG['conns_localhost'] = 0
CONFIG['conns_remote'] = 0
CONFIG['mode'] = None
CONFIG['GET_REQ_QUEUE'] = False
CONFIG['GET_DONE_QUEUE'] = False

class Stats:
    """Keep statistics describing what got moved."""

    def __init__(self):
        self.moved=0
        self.dup=0
        self.spam=0
        self.other=0
        self.workers=0
        self.retry=0
        self.notfound=0
        self.seenindb=0

    def addMoved(self):
        """Mark one article copied."""
        self.moved+=1

    def addDup(self):
        """Mark article duplicate (not copied)."""
        self.dup+=1

    def addSeen(self):
        """Mark article duplicate (not copied)."""
        self.seenindb+=1

    def addSpam(self):
        """Mark article unwanted (not copied)."""
        self.spam+=1

    def addOther(self):
        """Mark article not copied, but not duplicate."""
        self.other+=1

    def addNotFound(self):
        """Mark article not found at backend"""
        self.notfound+=1
        
    def addRetry(self):
        """Mark article to retry send to dest"""
        self.retry+=1

    def __str__(self):
        #text="Moved:  %d, Dup:  %d, Spam %d, Other:  %d, Conns (Local %d / Remote %d)" \
        #% (self.moved,self.dup,self.spam,self.other,CONFIG['conns_localhost'],CONFIG['conns_remote'])
        text="Moved:  %d, Seen: %d, Dup:  %d, Spam %d, Retry %d, NotF %d, Other %d @ final workers %d" \
        % (self.moved,self.seenindb,self.dup,self.spam,self.retry,self.notfound,self.other,CONFIG['running_workers'])
        return text

print("start class stats done")

######################################################################

DBINITSCRIPT="""
create table if not exists articles (
    messid varchar(256) primary key,
    ts timestamp,
    group_name varchar(256),
    status int
);

create table if not exists groups (
    group_name varchar(256) primary key,
    last_id int
);
"""

INS_ARTICLE="""insert or replace into articles values(?, ?, ?, ?)"""
GET_ARTICLE="""select * from articles where messid = ?"""
#GET_ARTICLE="""select * from articles where messid = ? and status = 'suc' or status = 'dup' or status = 'unw'"""
INS_GROUP="""insert or replace into groups values (?, ?)"""
GET_GROUP="""select * from groups where group_name = ?"""

class NewsDB:
    """Database of seen articles and groups.

    Entries for groups will have keys beginning with ``l/'' and entries for
    individual articles will have keys beginning with ``a/''

    Article entries (a) will have a string value that's a floating point number
    indicating the time that this article was copied.

    Group entries (l) have a string value that represents that last seen
    article by number for the given group (the part after the l/).
    """

    __TXN_SIZE = 10000

    def __init__(self, dbpath):
        try:
            self.db = sqlite.connect(dbpath)
            self.cur = self.db.cursor()
            self.cur.executescript(DBINITSCRIPT)
            self.log = logging.getLogger("NewsDB")
            self.__trans = 0
            self.__markArticles = True
        except Exception as e:
            self.log.warn("def NewsDB: __init__ failed, exception '%s'"%(e))
            sys.exit(1)

    def __maybeCommit(self):
        self.__trans += 1
        if self.__trans >= self.__TXN_SIZE:
            self.__trans = 0
            self.db.commit()

    def setShouldMarkArticles(self, to):
        """Set to false if articles should not be marked in the news db."""
        self.__markArticles=to

    def hasArticle(self, message_id):
        """Return true if there is a reference to the given message ID in
        this database.
        """
        rv=False
        if self.__markArticles:
            self.cur.execute(GET_ARTICLE, (message_id,))
            rv = len(self.cur.fetchall()) > 0
        return rv

    def markArticle(self, message_id, groupname, status):
        """Mark the article seen with group."""
        if self.__markArticles:
            #marked = False
            #try:
            self.cur.execute(INS_ARTICLE, (message_id, datetime.datetime.now(), groupname, status))
            self.__maybeCommit()
            self.log.debug("def markArticle: messid = '%s', group = '%s', status = '%s'"%(message_id, groupname, status))
            #except Exception as e:
            #    self.log.warn("def markArticle: failed, exception = '%s'"%(e))
            #    sys.exit(1)
            #finally:
            #    return marked
        else:
            self.log.debug("def markArticle: disabled, messid = '%s', group = '%s', status = '%s'"%(message_id, groupname, status))
        return True
                
    def getLastId(self, group):
        """Get the last seen article ID for the given group, or 0 if this
        group hasn't been seen.
        """
        rv=0
        rows=None
        self.cur.execute(GET_GROUP, (group,))
        rows = self.cur.fetchall()
        if rows != None:
            try:
                rv = int(rows[0][1])
            except Exception as e:
                # def getLastId: #1 exception = 'list index out of range'
                if str(e).startswith("list index out of range"):
                    pass
                else:
                    self.log.warn("def getLastId: #1 exception = '%s', group='%s' rows='%s', rv='%s'"%(e,group,rows,rv))
        self.log.debug("def getLastId: grp=%s, rows=%s, rv=%s"%(group,rows,rv))
        return rv

    def setLastId(self, group, idx):
        """Set the last seen article ID for the given group."""
        self.cur.execute(INS_GROUP, (group, idx))
        self.__maybeCommit()
        return True

    def __del__(self):
        """Close the DB on destruct."""
        self.db.commit()
        self.cur.close()
        self.db.close()

    def getGroupRange(self, group, first, last, maxArticles=0):
        """Get the group range for the given group.

        The arguments represent the group you're looking to copy, and the
        first and last article numbers as provided by the news server.

        The first, last, and count that should be checked will be returned
        as a tuple."""

        # Start with myfirst being one greater than the last thing we've seen
        myfirst=self.getLastId(group) + 1
        first=int(first)
        last=int(last)
        self.log.debug("def getGroupRange: '%s' ranges from %d-%d, we want %d" % (group, first, last, myfirst))
        if (myfirst < first) or (myfirst > (last+1)):
            myfirst=first
            self.log.debug("def getGroupRange: Our first was out of range, now we want %d" % (myfirst, ))
        mycount=(last-myfirst)+1
        
        
        self.log.debug("def getGroupRange: maxArticles %d, mycount %d, myfirst %d" % (maxArticles, mycount, myfirst))
        

        if maxArticles > 0 and mycount > maxArticles:
            self.log.warn("def getGroupRange: Want %d articles with a max of %d...shrinking" % (mycount, maxArticles))
            myfirst = myfirst + (mycount - maxArticles)
            mycount = maxArticles
            self.log.warn("def getGroupRange: New count is %d, starting with %s" % (mycount, myfirst))
        
        if mycount > 0:
            self.log.debug("def getGroupRange: '%s' found=%d start=%d" % (group, mycount, myfirst))
        
        return myfirst, last, mycount

print("start class NewsDB done")
######################################################################
print("start class NNTPClient ")
class NNTPClient(nntplib.NNTP):
    """An extension of nntplib.NNTP suitable for...well, it actually
    works."""

    headers=['From', 'Subject', 'Message-Id', 'Sender', 'MIME-Version', \
        'Path', 'Newsgroups', 'Organization', 'Approved', 'Sender', \
        'Distribution', \
        'Lines', 'Content-Type', 'Content-Transfer-Encoding']

    def __init__(self, host, port=119,user=None,password=None,readermode=True):
        """See netlib.NNTP"""
        global CONFIG
        self.log=logging.getLogger("NNTPClient")

        if not self.checkMode():
            self.log.warn("NNTPClient) __init__ failed, checkMode()")
            return False
        
        self.log.info("NNTPClient) __init__ Connecting to %s:%d" % (host, port))
        try:
            if CONFIG['mode'] != "reader":
                readermode = False
            else:
                readermode = True
            
            nntplib.NNTP.__init__(self, host, port, user, password, readermode)
            self.host=host
            self.port=port
            """
            if host == '127.0.0.1' or host == "localhost":
                CONFIG['conns_localhost'] += 1
            else:
                CONFIG['conns_remote'] += 1
            """
            self.log.info("NNTPClient) __init__ Auth OK: %s:%d" % (self.host, self.port))
        except nntplib.NNTPPermanentError,e:
            self.log.warn("NNTPClient) __init__ failed NNTPPermanentError, exception = '%s'"%(e))
            return False
        except Exception as e:
            self.log.warn("NNTPClient) __init__ failed, exception = '%s'"%(e))
            return False
        
        self.log.info("NNTPClient) __init__ done")

    def __repr__(self):
        return ("<NNTPClient: " + self.host + ":" + `self.port` + ">")

    def checkMode(self):
        """Upon construct, this tries to figure out whether the server
        considers us a feeder or a reader."""
        self.currentmode = CONFIG['mode']
        self.log.info("Detected mode %s" % (self.currentmode))
        return True
        

    def __headerMatches(self, h):
        """Internal, checks to see if the header ``h'' is in our list of
        approved headers."""
        rv=None
        for header in self.headers:
            if h.lower().find(header.lower()) == 0:
                rv=1
        return rv

    def ihave(self, id):
        try:
            """Send an IHAVE for a message ID (feeder mode only).
            An exception will be raised if something breaks, or the article
            isn't wanted.  If an exception is not raised, follow with a stream
            of the article."""
            self.log.debug("def ihave: '%s' "%(id))
            resp = self.shortcmd('IHAVE ' + id)
            #self.log.info("def ihave: IHAVE '%s', resp='%s' "%(id,resp))
            if resp.startswith("335 "):
                self.log.debug("def ihave: IHAVE '%s', resp='%s' "%(id,resp))
                return True
            else:
                self.log.warn("def ihave: IHAVE '%s', resp='%s' "%(id,resp))
        except Exception as e:
            
            if str(e).startswith("435 "):
                # "435 duplicate"
                return 435
            elif str(e).startswith("436 "):
                # 436 Expiring process
                return 436
            else:
                self.log.warn("def ihave: failed, exception = '%s', id = '%s'" % (e,id))
        return False

    def copyArticle(self, src, which, messid, group):
        """Copy an article from the src server to this server.
        which is the ID of the message on the src server, messid is the
        message ID of the article."""
        self.log.debug("def copyArticle: Moving which='" + str(which) + "' messid='" + messid + "', grp='%s'"%(group))
        pgrp = group.replace(".","/")

        if self.currentmode == 'reader' or self.currentmode == 'reader1':
            self.log.debug("def copyArticle: read messid = '%s'"%messid)
            try:
                src.article(str(which))
                self.log.debug("def copyArticle: srced article '%s', messid='%s', grp='%s'"%(which,messid,group))
                return True
            except IOError, e:
                self.log.warn("def copyArticle: failed messid='%s' IOError='%s', 'fix %s/.art%s'"%(messid,e,pgrp,which))
                writebadarticle(group,which,messid,pgrp)
            except EOFError, e:
                self.log.warn("def copyArticle: failed messid='%s' EOFerror='%s', 'fix %s/.art%s'"%(messid,e,pgrp,which))
                writebadarticle(group,which,messid,pgrp)
            except Exception as e:
                if str(e).startswith("423 "):
                    self.log.debug("def copyArticle: failed src.article grp='%s', num='%s', messid='%s', exception '%s'" % (group,which,messid,e))
                    # 423 Article number not in this group
                    return 423
                else:
                    self.log.warn("def copyArticle: failed src.article grp='%s', num='%s', messid='%s', exception '%s'" % (group,which,messid,e))

        elif self.currentmode == 'ihave':
            self.log.debug("def copyArticle: IHAVE messid = '%s'"%messid)
            # ask first and source if needed or send break if not found
            valihave = self.ihave(messid)
            if valihave == True:
                self.log.debug("def copyArticle: ihave True, article '%s', messid='%s', grp='%s', send it"%(which,messid,group))
                try:
                    #resp, nr, nid, lines = src.article(messid)
                    resp, nr, nid, lines = src.article(str(which))
                    if resp == None:
                        self.log.info("def copyArticle: resp=None, messid='%s', grp='%s'"%(which,messid,group))
                        return False
                    #self.log.info("def copyArticle: ihave srced article '%s', messid='%s', grp='%s'"%(which,messid,group))
                    #self.log.info("def copyArticle: ihave srced article '%s', messid='%s', grp='%s' resp='%s'"%(which,messid,group,resp))
                    """
                    len_lines = len(lines)
                    if len_lines > 1500:
                        self.log.info("def copyArticle: prefilter unwanted 437 len(lines)=%s" % len_lines)
                        return 437
                    """
                    try:
                        valtakethis = self.takeThis(messid, lines)
                        if valtakethis == True:
                            self.log.debug("def copyArticle: sent messid = '%s' OK" % (messid))
                            return True
                        elif valtakethis == 437:
                            # 437 unwanted
                            self.log.debug("def copyArticle: unwanted 437 len(lines)=%s" % len(lines))
                            return 437
                        elif valtakethis == 436:
                            # 436 retry later
                            return 436
                        else:
                            self.log.info("def copyArticle: NOT sent messid = '%s', valihave = %s" % (messid,valihave))
                            return False
                    except Exception as e:
                        self.log.warn("def copyArticle failed: self.takeThis(%s), exception = '%s'" % (messid,e))
                        return False
                        
                except nntplib.NNTPTemporaryError, e:
                    # Generate an error, I don't HAVE this article, after all
                    self.log.debug("def copyArticle: Did not have messid='%s' grp=%s e='%s'"%(messid,group,e))
                    try:
                        resp = self.shortcmd('\r\n.')
                        self.log.debug("def copyArticle: try '%s' shortcmd '\r\n.', resp = '%s'" %(messid,resp))
                        if str(e).startswith("423 "):
                            # 423 Article number not in this group
                            return 423
                    except nntplib.NNTPTemporaryError, e:
                        # 437 Empty headers and body
                        if str(e).startswith("437 "):
                            return 437
                        else:
                            self.log.warn("def copyArticle failed (ihave) nntplib.NNTPTemporaryError, exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                    except Exception as e:
                        self.log.warn("def copyArticle failed (ihave) shortcmd, exception = '%s', which = '%s', messid='%s'" % (e,which,messid))

                except nntplib.NNTPDataError, e:
                    self.log.warn("def copyArticle failed (ihave), NNTPDataError exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                except IOError, e:
                    self.log.warn("def copyArticle failed (ihave), IOError exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                    writebadarticle(group,which,messid,pgrp)
                except EOFError, e:
                    self.log.warn("def copyArticle failed (ihave), EOFError exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                    writebadarticle(group,which,messid,pgrp)
                except Exception as e:
                    self.log.warn("def copyArticle failed (ihave), exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                    if str(e).startswith("423 "):
                        # 423 Article number not in this group
                        return False
            elif valihave == 435:
                # ihave returned 435 duplicate
                return 435
            elif valihave == 436:
                # 436 retry later
                return 436
            else:
                # ihave returned false
                return False

        elif self.currentmode == 'ihave2':
            self.log.debug("def copyArticle: IHAVE2 messid = '%s'"%messid)
            # source first and ask if wanted
            try:
                #resp, nr, nid, lines = src.article(messid)
                resp, nr, nid, lines = src.article(str(which))
                self.log.debug("def copyArticle: ihave2 srced article '%s', messid='%s', grp='%s'"%(which,messid,group))
                try:
                    valihave = self.ihave(messid)
                    if valihave == True:
                        valtakethis = self.takeThis(messid, lines)
                        if valtakethis == True:
                            self.log.debug("def copyArticle: ihave2 sent messid = '%s' OK" % (messid))
                            return True
                        elif valtakethis == 437:
                            # 437 unwanted
                            return 437
                        else:
                            self.log.debug("def copyArticle: ihave2 NOT sent messid = '%s'" % (messid))
                            return False
                    elif valihave == 435:
                        # 435 duplicate
                        return 435
                    elif valihave == 436:
                        # 436 retry later
                        return 436
                    else:
                        return False
                except Exception as e:
                    self.log.warn("def copyArticle failed: ihave2 takeThis messid='%s', exception = '%s'" % (messid,e))
                    return False
                    
            except IOError, e:
                self.log.warn("def copyArticle failed (ihave2), IOError exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                writebadarticle(group,which,messid,pgrp)
            except EOFError, e:
                self.log.warn("def copyArticle failed (ihave2), EOFError exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                writebadarticle(group,which,messid,pgrp)
            except Exception as e:                
                if str(e).startswith("423 "):
                    # 423 Article number not in this group
                    return False
                else:
                    self.log.warn("def copyArticle failed (ihave2) src.article, exception = '%s', which = '%s', messid='%s'" % (e,which,messid))
                        
        elif self.currentmode == 'post':
            self.log.warn("def copyArticle failed (post) not implemented")
            
        # copyArticle should not end here so we better take a break
        self.log.warn("def copyArticle failed and worker broken group='%s', which = '%s', messid = '%s', host = '%s'"%(group,which,messid,self.host))
        return None
        #sys.exit(1)
        """
        try:
            self.log.warn("def copyArticle try reconnect worker")
            self.dest = self.destf()
        except Exception as e:
            self.log.warn("def copyArticle try reconnect worker failed, e = '%s'"%(e))
            sys.exit(1)
        """

    def takeThis(self, messid, lines):
        try:
            """Stream an article to this server."""
            self.log.debug("def takeThis: messid = '%s', lines='%d'" % (messid,len(lines)))
            """
            if len(lines) > 1500:
                self.log.info("def takeThis: prefilter messid = '%s', lines='%d'" % (messid,len(lines)))
                return 437
            """
            for l in lines:
                if l == '.':
                    self.log.debug("*** L was ., adding a dot. ***")
                    l = '..'
                self.putline(l)
            self.putline('.')
            self.getresp()
            return True
        except Exception as e:
            # 437 (unwanted message)
            if str(e).startswith("437 "):
                self.log.debug("def takeThis messid = '%s' e='%s'" % (messid,e))
                return 437
            else:
                self.log.warn("def takeThis failed, exception = '%s', messid = '%s'" % (e,messid))
        return False

    def post(self, lines):
        #try:
        """Post an article to this server."""
        self.log.debug("def post: lines='%d'" % (len(lines)))
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
        #except Exception as e:
        #    self.log.warn("def post failed, exception = '%s'" % (e))

print("start class nntpclient done")
######################################################################
print("start class worker")

class Worker(threading.Thread):
    ### self.workers = [Worker(srcf, destf, self.reqQueue, self.doneQueue)
    def __init__(self, sf, df, inq, outq):
        global CONFIG
        threading.Thread.__init__(self)
        self.log=logging.getLogger("Worker")
        self.log.info("Worker) def __init__ ()")
        self.srcf = sf
        self.destf = df
        self.inq = inq
        self.outq = outq
        self.currentGroup = None
        self.running = True
        self.setName("worker")
        self.setDaemon(True)
        self.log.info("Worker) setDaemon True, start()")
        self.start()
        self.log.info("Worker) started")
        CONFIG['running_workers'] += 1

    def run(self):
        self.log.info("Worker) def run()")
        y = True
        #while self.running == True:
        if y:
            try:
                self.src = self.srcf()
                self.dest = self.destf()
                self.log.info("Worker) def run: self.src = '%s', self.dest = '%s', name='%s' join mainLoop()"%(self.src,self.dest,self.getName))
                self.mainLoop()
                self.log.debug("Worker) def run: mainLoop() END self.running = '%s'"%(self.running))
            except Exception as e:
                traceback.print_exc()
                self.log.debug("Worker) def run: mainLoop() exception = '%s'"%(e))
                sys.exit(1)

        
        global CONFIG
        if CONFIG['running_workers'] > 0:
            CONFIG['running_workers'] -= 1
        
        if CONFIG['running_workers'] == 0:
            self.running = False
            iqobjs = self.inq.qsize()
            oqobjs = self.outq.qsize()
            self.log.warn("def run: NO WORKERS LEFT! inq.size='%s', outq.size='%s'"%(iqobjs,oqobjs))
            
            if iqobjs > 0:
                # clean inq
                self.log.warn("def run: cleanup inq %s todo"%(iqobjs))
                i = 0
                while self.inq.qsize() > 0:
                    try:
                        group, num, messid = self.inq.get_nowait()
                        i+=1
                    except Queue.Empty:
                        break
                if i > 0:
                    self.log.warn("def run: cleaned inq %s/%s"%(i,iqobjs))

            
            if oqobjs > 0:
                # clean outq
                self.log.warn("def run: cleanup outq %s todo"%(oqobjs))
                i = 0
                while self.outq.qsize() > 0:
                    try:
                        t,  messid, groupname, idx = self.outq.get_nowait()
                        i+=1
                    except Queue.Empty:
                        break
                if i > 0:
                    self.log.warn("def run: cleaned outq %s/%s"%(i,oqobjs))
            
        # end if workers == 0
        self.inq.task_done()
        self.outq.task_done()
        self.log.info("def run: running Workers = '%s', inq.size='%s', outq.size='%s' self.running = %s QUIT"%(CONFIG['running_workers'],self.inq.qsize(),self.outq.qsize(),self.running))
        

    def mainLoop(self):
        while self.running == True:
            if CONFIG['running_workers'] == 0:
                return False
            num = None
            group = None
            resp = None

            try:
                #self.log.debug("def mainLoop: inq.get queue GET grp='%s'"%(group))
                group, num, messid = self.inq.get_nowait()
                #self.log.debug("def mainLoop: inq.get queue GOT grp='%s', num = '%s', messid = '%s'"%(group,num,messid))
            except Queue.Empty:
                #self.log.debug("def mainLoop: inq.get queue empty, grp='%s', pass"%(group))
                time.sleep(0.1)
                continue
                
            if num != None:
                self.log.debug("Worker) def mainLoop: doing %s %s, %s, %s" % (CONFIG['mode'], group, num, messid))
                if group != self.currentGroup:
                    try:
                        self.src.group(group)
                        self.currentGroup = group
                    except Exception as e:
                        self.log.warn("Worker) def mainLoop: try #0 self.src.group(group) failed, exception = '%s', group='%s', messid='%s'"%(e,group,messid))
                        sys.exit(1)

                if group == self.currentGroup:
                    try:
                        valcopyart = self.dest.copyArticle(self.src, num, messid, group)
                        if valcopyart == None:
                            self.log.warn("def mainLoop: copyArticle returned None grp='%s', num = '%s', messid = '%s', break mainloop of worker"%(group,num,messid))
                            self.running = False
                        if valcopyart == True:
                            self.log.debug("Worker) def MainLoop copyArticle: OK num='%s' messid='%s', grp='%s', inq.size='%s', outq.size='%s'"%(num,messid,group,self.inq.qsize(),self.outq.qsize()))
                            self.outq.put(('success', messid, group, num))
                            self.log.debug("Worker) def MainLoop copyArticle: outq.put=success num='%s' messid='%s', grp='%s', inq.size='%s', outq.size='%s'"%(num,messid,group,self.inq.qsize(),self.outq.qsize()))
                        elif valcopyart == 423:
                            self.outq.put(('notfound', messid, group, num))
                        elif valcopyart == 435:
                            self.outq.put(('duplicate', messid, group, num))
                        elif valcopyart == 436:
                            self.outq.put(('retry', messid, group, num))
                        elif valcopyart == 437:
                            self.outq.put(('unwanted', messid, group, num))
                        else:
                            self.outq.put(('error', messid, group, num))
                            self.log.debug("Worker) def MainLoop copyArticle: error grp='%s', messid='%s', num='%s'"%(group,messid,num))
                        """
                    except nntplib.NNTPTemporaryError, e:
                        if str(e).find("Duplicate"):
                            self.outq.put(('duplicate', messid))
                            self.log.warn("Worker) def mainLoop: Duplicate, NNTPTemporaryError exception = '%s', group='%s', messid='%s', artnum = %s"%(e,group,messid,num))
                        else:
                            self.log.warn("Worker) def mainLoop: failed n1, NNTPTemporaryError exception = '%s', group='%s', messid='%s', artnum = %s"%(e,group,messid.num))
                        """
                    except nntplib.NNTPProtocolError, e:
                        self.log.warn("Worker) def mainLoop: failed n2, NNTPProtocolError exception = '%s', group='%s', messid='%s', artnum = %s"%(e,group,messid,num))

                    except EOFError, e:
                        self.log.warn("Worker) def mainLoop: failed n3, exception = '%s', group='%s', messid='%s'"%(e,group,messid))
                        #sys.exit(1)

                    except IOError, e:
                        self.log.warn("Worker) def mainLoop: failed n4, exception = '%s', group = '%s', messid = '%s'"%(e,group,messid))
                        #sys.exit(1)
            
            try:
                self.log.debug("Worker) def mainLoop: inq.task_done() num='%s', messid='%s', grp='%s'"%(num,messid,group))
                #self.inq.task_done()
            except Exception as e:
                self.log.warn("Worker) def mainLoop: exception inq.task_done() failed, exception = '%s', group = '%s'"%(e,group))
            
            if self.running == False:
                self.log.warn("Worker) def mainLoop: running = False, escape while")
                break
        if self.running == False:
            self.log.warn("Worker) def mainLoop: running = False, return False")
            return False

print("start class worker done")
##########################################################
print("start class nntpsucka")
class NNTPSucka:
    """Copy articles from one NNTP server to another."""

    def __init__(self, srcf, destf, config):
        """Get an NNTPSucka with two NNTPClient objects representing the
        source and destination."""
        self.log=logging.getLogger("NNTPSucka")

        self.runningNS = True
        
        try:
            self.src=srcf()
        except Exception as e:
            self.log.warn("NNTPsucka) def __init__: self.src=srcf() failed, exception = '%s'" %(e))
            sys.exit(1)
            
        try:
            self.dest=destf()
        except Exception as e:
            self.log.warn("NNTPsucka) def __init__: self.dest=destf() failed, exception = '%s'" %(e))
            sys.exit(1)
            
        self.log.info("NNTPsucka) def __init__: self.src = '%s', self.dest = '%s'" %(self.src,self.dest))
        self.config = config

        self.workersnum = config.getint("misc", "workers")
        
        self.reqQueue = Queue.Queue(10000)
        self.doneQueue = Queue.Queue(10000)
                
        # Figure out the maximum number of articles per group
        self.log.info("NNTPsucka: SETTING maxArticles")
        self.maxArticles=config.getint("misc", "maxArticles")
        self.log.debug("Max articles is configured as %d" %(self.maxArticles))

        # NewsDB setup
        
        self.log.info("NNTPsucka: SETTING newsdb")
        self.db=NewsDB(config.get("misc","newsdb"))
        self.log.info("NNTPSucka) Config: newsdb=%s"%(config.get("misc","newsdb")))

        self.log.info("NNTPsucka: SETTING shouldMarkArticles")
        self.db.setShouldMarkArticles(config.getboolean("misc","shouldMarkArticles"))
        
        
        # Initialize stats
        self.stats=Stats()

        # Start Workers
        
        self.log.info("NNTPsucka) def __init__: start workers")
        self.workers = [Worker(srcf, destf, self.reqQueue, self.doneQueue)
                        for x in range(self.workersnum)]
        
      
    def copyGroup(self, groupname):
        global CONFIG
        if CONFIG['running_workers'] == 0:
            self.log.warn("def copyGroup: group='%s', no workers!"%(groupname))
            return False
        self.log.debug("def copyGroup: group='%s' workers = '%s'"%(groupname,CONFIG['running_workers']))
        
        """Copy the given group from the source server to the destination
        server.
        Efforts are made to ensure only articles that haven't been seen are
        copied."""
        self.log.debug("def copyGroup(): from src grp=%s"%(groupname))
        pgrp = groupname.replace(".","/")

        if CONFIG['mode'] == "mbox":
            self.log.debug("def copyGroup(): mode mbox")
            
            GPF = groupname.split(".")[0]
            ZIPPATH = "%s/usenet-%s"%(MBOX_DIR,GPF)
            ZIPFILE = "%s/%s.mbox.zip"%(ZIPPATH,groupname)
            BOXFILE = "%s/%s.mbox"%(ZIPPATH,groupname)
            mbox = None
            i=0


            if not os.path.isfile(BOXFILE) and os.path.isfile(ZIPFILE):
                self.log.info("def copyGroup(): ZIP %s FOUND"%(ZIPFILE))
                with ZipFile(ZIPFILE, 'r') as zip:
                    zip.extract("%s.mbox"%groupname,ZIPPATH)

            if os.path.isfile(BOXFILE):
                self.log.info("def copyGroup(): MBOX FOUND %s"%(BOXFILE))
                try:
                    mbox = mailbox.mbox(BOXFILE)
                    for message in mbox:
                         i+=1
                    self.log.info("def copyGroup(): MBOX %s = %s messages"%(BOXFILE,i))
                except Exception as e:
                    self.log.warn("def copyGroup(): MBOX failed, exception = %s"%(e))
            mbox = None
            return
        else:
            pass
        
        try:
            # 211 7531 2 7532 fr.comp.applications.genealogie group selected
            resp, count, first, last, name = self.src.group(groupname)
            self.log.debug("def copyGroup(): self.src.group == resp='%s', count=%s, first=%s, last=%s, name=%s"%(resp,count,first,last,name))
        except IOError, e:
            if not "Broken pipe" in str(e):
                self.log.warn("def copyGroup(): IOError grp='%s', exception = '%s', path=%s))"%(groupname,e,pgrp))
            CONFIG['running_workers'] -= 1
            #sys.exit(1)
            return False
        except EOFError, e:
            self.log.warn("def copyGroup(): EOFError grp='%s', exception = '%s', path=%s))"%(groupname,e,pgrp))
            CONFIG['running_workers'] -= 1
            #sys.exit(1)
            return False
        except Exception as e:
            self.log.warn("def copyGroup(): failed grp = '%s', exception = '%s', path=%s))"%(groupname,e,pgrp))
            writetoBadGroupsList(groupname)
            return False
            #sys.exit(1)
        
        # Figure out where we are
        myfirst, mylast, mycount = self.db.getGroupRange(groupname, first, last, self.maxArticles)
        l = []
        if mycount > 0:
            self.log.info("def copyGroup: Checking " + `mycount` + " articles:  " + `myfirst` + "-" + `mylast` + " in " + groupname)
            
            #y = True
            #if y:
            try:
                # Grab the IDs
                self.log.info("def copyGroup: run self.src.xhdr grp='%s' myfirst %s, mylast %s"%(groupname,myfirst,mylast))
                resp, l = self.src.xhdr('message-id', `myfirst` + "-" + `mylast`)
                self.log.info("def copyGroup: got self.src.xhdr grp='%s', resp=%s l='%s'"%(groupname,resp,len(l)))
            except Exception as e:
                self.log.warn("def copyGroup: self.src.xhdr(message-id, myfirst=%s - mylast=%s) failed, exception = '%s'"%(myfirst,mylast,e))
                return False
                
            # Validate we got as many results as we expected.
            if(len(l) != mycount):
                # WARNING Unexpected number of articles returned.  Expected 238418, but got 238417 groupname='sci.astro'
                self.log.warn("Unexpected number of articles returned.  " + "Expected " + `mycount` + ", but got " + `len(l)` + " groupname=" + `groupname`)

        # Flip through the stuff we actually want to process.
        self.succ, self.suct = 0, 0
        self.dupp, self.dupt = 0, 0
        self.errs, self.errt = 0, 0
        self.seen, self.seet = 0, 0
        self.unww, self.unwt = 0, 0
        self.retr, self.rett = 0, 0
        self.notf, self.nott = 0, 0
        self.hasput, self.hasputt = 0, 0
        self.lent = len(l)
        self.lentf = len(l)
        self.processed = 0

        self.groupstarttime = int(round(time.time() * 1000))
        self.processtime = int(round(time.time() * 1000))
        
        
        if self.lent == 0:
            self.log.debug("def copyGroup: grp='%s' lent %s, writetoDoneList, return True"%(groupname,self.lent))
            writetoDoneList(groupname)
            return True

        preprocess = 0
        for i in l:
            if CONFIG['running_workers'] == 0:
                self.log.warn("def copyGroup: for i in l: group='%s', no workers!"%(groupname))
                return False
            idx=i[0]
            messid=i[1]
            assert(int(idx) >= myfirst and int(idx) <= mylast)
            
            if self.db.hasArticle(messid):
                # check if article is in DB
                self.log.debug("def copyGroup: seenindb doneQueue.put grp='%s', messid=%s"%(groupname,messid))
                self.doneQueue.put(('seenindb', messid, groupname, idx))
                self.hasput += 1
                self.hasputt += 1
            elif CONFIG['running_workers'] > 0:
                # put article to fetch into queue: reqQueue / inq
                self.log.debug("def copyGroup: reqQueue.put grp='%s', messid=%s"%(groupname,messid))
                self.reqQueue.put((groupname, idx, messid))
                self.hasput += 1
                self.hasputt += 1
            if self.hasput > 9999:
                self.log.debug("def copyGroup: grp='%s' self.hasputt = %s / %s"%(groupname,self.hasputt,self.lent))
                # preprocess group if there are more than 1k articles to fetch
                if self.lent > 9999:
                    while preprocess < self.hasput:
                        if CONFIG['running_workers'] == 0:
                            return False
                        if self.processor(groupname,block=False):
                            preprocess += 1
                    preprocess = 0
                self.hasput = 0

        if self.hasput > 0:
            self.log.debug("def copyGroup: grp='%s' self.hasputt = %s / %s"%(groupname,self.hasputt,self.lent))
        
        #for i in l:
        while self.processed < self.lent:
            if CONFIG['running_workers'] == 0:
                return False
            self.processor(groupname,block=False)

        self.log.debug("def copyGroup: grp='%s' processed %s / %s"%(groupname,self.processed,self.lent))

        if self.processed != self.lent:
            self.log.warn("def copyGroup: grp='%s' processed %s != lent %s"%(groupname,self.processed,self.lent))
            return False
            
        try:
            if self.suct > 0 or self.seet > 0 or self.dupt > 0 or self.rett > 0 or self.nott > 0 or self.errt > 0:
                self.groupsendtime = int(round(time.time() * 1000))
                self.groupruntime = self.groupsendtime-self.groupstarttime
                self.log.info("def copyGroup: '%s' added %d/%d, seen %d, dupt %d, rett %d, nott %d, errt %d"%(groupname,self.suct,self.lent,self.seet,self.dupt,self.rett,self.nott,self.errt))
                writetoDoneList(groupname)
                return True
        except Exception as e:
            self.log.warn("def copyGroup: failed final group '%s', exception = '%s'"%(groupname,e))

       
    def processor(self,groupname,block):
        try:
            #self.log.debug("def processor: doneQueue try get job grp '%s'"%(groupname))
            t,  messid, groupname, idx = self.doneQueue.get(block)
            #self.log.debug("def processor: doneQueue got job grp='%s', messid='%s'"%(groupname,messid))
                      
            if t == 'success':
                status = "suc"
                if self.db.markArticle(messid, groupname, status):
                    # Mark this message as having been read in the group
                    self.db.setLastId(groupname, idx)
                    self.stats.addMoved()
                    self.log.debug("def processor: grp='%s', success @ messid = '%s' (%s/%s) %s"%(groupname,messid,self.suct,self.lent,self.lentf))
                    self.succ += 1
                    self.suct += 1
                    #self.lentf -= 1
                    #if self.lent <= 999 and self.suct <= 10:
                    #    self.log.info("def processor: grp='%s' success %s (%d/%d) %s workers=%s"%(groupname,CONFIG['mode'],self.suct,self.lent,self.lentf,CONFIG['running_workers']))
                    #if (self.lent <= 9999 and self.succ > 999) or (self.lent > 9999 and self.succ > 9999):
                    if self.succ > 999:
                        now = int(round(time.time() * 1000))
                        spent = now-self.processtime
                        speed = 0
                        try:
                            speed = round(self.succ/spent*1000)
                        except Exception as e:
                            speed = e
                        self.log.info("def processor: grp='%s' success %s (%d/%d) %s wrk=%s [%s art/s]"%(groupname,CONFIG['mode'],self.suct,self.lent,self.lentf,CONFIG['running_workers'],speed))
                        self.succ = 0
                        self.processtime = int(round(time.time() * 1000))
                        

            elif t == 'duplicate':
                status = "dup"
                if self.db.markArticle(messid, groupname, status):
                    # Mark this message as having been read in the group
                    self.db.setLastId(groupname, idx)
                    self.stats.addDup()
                    self.log.debug("def processor: grp='%s', duplicate @ messid = '%s'"%(groupname,messid))
                    self.dupp += 1
                    self.dupt += 1
                    if self.dupp > 999:
                        self.log.info("def processor: grp='%s', duplicate %s %s (%d/%d) %s"%(groupname,CONFIG['mode'],self.dupt,self.suct,self.lent,self.lentf))
                        self.dupp = 0
            
            elif t == 'unwanted':
                status = "unw"
                if self.db.markArticle(messid, groupname, status):
                    # Mark this message as having been read in the group
                    self.db.setLastId(groupname, idx)
                    self.stats.addSpam()
                    self.log.debug("def processor: grp='%s', unwanted @ messid = '%s'"%(groupname,messid))
                    self.unww += 1
                    self.unwt += 1
                    if self.unww > 999:
                        self.log.info("def processor: grp='%s', unwanted %s %d (%d/%d) %s"%(groupname,CONFIG['mode'],self.unwt,self.suct,self.lent,self.lentf))
                        self.unww = 0

            elif t == 'retry':
                status = "ret"
                if self.db.markArticle(messid, groupname, status):
                    self.stats.addRetry()
                    self.log.debug("def processor: grp='%s', retry @ messid = '%s'"%(groupname,messid))
                    self.retr += 1
                    self.rett += 1
                    if self.retr > 999:
                        self.log.info("def processor: grp='%s', retry %s %d (%d/%d) %s"%(groupname,CONFIG['mode'],self.rett,self.suct,self.lent,self.lentf))
                        self.retr = 0

            elif t == 'notfound':
                status = "nof"
                if self.db.markArticle(messid, groupname, status):
                    self.stats.addNotFound()
                    self.log.debug("def processor: grp='%s', notfound @ messid = '%s'"%(groupname,messid))
                    self.notf += 1
                    self.nott += 1
                    if self.notf > 99:
                        self.log.info("def processor: grp='%s', notfound %s %d (%d/%d) %s"%(groupname,CONFIG['mode'],self.nott,self.suct,self.lent,self.lentf))
                        self.notf = 0

            elif t == 'seenindb':
                self.stats.addSeen()
                # Mark this message as having been read in the group
                self.db.setLastId(groupname, idx)
                status = "see"
                self.seen += 1
                self.seet += 1
                if self.seen > 999:
                    self.log.info("def processor: grp='%s', seenindb %s %d (%d/%d) %s"%(groupname,CONFIG['mode'],self.seet,self.suct,self.lent,self.lentf))
                    self.seen = 0                    
            else:
                status = "err"
                if self.db.markArticle(messid, groupname, status):
                    self.stats.addOther()
                    self.log.debug("def processor: '%s', error @ messid = '%s' t='%s'"%(groupname,messid,t))
                    self.errs += 1
                    self.errt += 1
                    if self.errs > 1:
                        self.log.info("def processor: grp='%s' errors %s %d/%d"%(groupname,CONFIG['mode'],self.errt,self.lent))
                        self.errs = 0

            
            self.processed += 1
            self.lentf -= 1
            return True
            
        except Queue.Empty:
            #self.log.debug("def copyGroup: done.queue empty, grp='%s', pass"%(groupname))
            time.sleep(1)
            
        except Exception as e:
            self.log.warn("def copyGroup: failed Queue '%s', exception = '%s'"%(groupname,e))
        
        return False
        

    def shouldProcess(self, group):
        self.log.debug("def shouldProcess: grp='%s'"%(group))

        ignorelist = CONFIG['filterList']
        globalignorelist = CONFIG['globalfilterList']
        forcedlist = CONFIG['forcedList']
        donelist = CONFIG['doneList']

        inforcedlist = False
        if forcedlist != None:
            self.log.debug("def sP: using forcedlist, len = '%s'"%(len(forcedlist)))
            for i in forcedlist:
                try:
                    if i.match(group) is not None:
                        self.log.debug("def sP: group '%s' in forcedlist"%(group))
                        inforcedlist = True
                        break
                except Exception as e:
                    self.log.warn("def sP: failed exception = %s"%e)
        else:
            self.log.debug("def shouldProcess: group '%s', else YES"%(group))
            return True

        if inforcedlist == False:
            self.log.debug("def shouldProcess: group '%s' not in forcedlist"%(group))
            return False

        if donelist != None:
            self.log.debug("def sP: using donelist, len = '%s'"%(len(donelist)))
            if group in donelist:
                self.log.debug("def sP: group '%s' in donelist"%(group))
                return False

            
        if globalignorelist != None:
            self.log.debug("def sP: using globalignorelist, len = '%s'"%(len(globalignorelist)))
            for i in globalignorelist:
                if i.match(group) is not None:
                   self.log.debug("def sP: group '%s' in globalignorelist"%(group))
                   return False
        
        
        if CONFIG['useIgnore'] == True:
            if ignorelist != None:
                self.log.debug("def sP: using ignorelist, len = '%s'"%(len(ignorelist)))
                for i in ignorelist:
                    if i.match(group) is not None:
                        self.log.info("def sP: group '%s' in ignorelist"%(group))
                        return False


        self.log.debug("def shouldProcess: group '%s', end False"%(group))
        return True

    
    def copyServer(self):
        """Copy all groups that appear on the destination server to the
        destination server from the source server."""
        self.log.info("def copyServer: Getting list of groups from " + `self.dest`)
        if CONFIG['mode'] == "mbox":
            pass
        else:
            pass
            
        list = []
        try:
            resp, list = self.dest.list()
        except Exception as e:
            self.log.warn("def copyServer: getting list from dest failed, exception = '%s'"%(e))
            sys.exit(1)

        
        if len(list) == 0:
            self.log.warn("Got list 0 from dest")
            sys.exit(1)
        else:
            self.log.debug("def copyServer: got list of %s groups from destination"%(len(list)))
        #g=group to fetch
        #s=groups shouldProcess
        #f=groups filtered
        #o=groups fetched ok
        g,s,f,o = 0,0,0,0
        groups = []

        for l in list:
            group=l[0]
            if self.shouldProcess(group):
                groups.append(group)
                s += 1
            else:
                f += 1

        if len(groups) == 0:
            self.log.warn("def copyServer: no groups to process")
            return False
        else:
            self.log.info("def copyServer: %s groups to process"%(len(groups)))
            
        for group in groups:
            g += 1
            self.log.info("def copyServer: DOING grp='%s' (%s/%s) ok=%s tot=%s fil=%s wrks=%s" % (group,g,s,o,len(list),f,CONFIG['running_workers']))
            try:
                if CONFIG['running_workers'] == 0:
                    return False
                elif self.copyGroup(group):
                    o += 1
            except Exception as e:
                self.log.warn("def copyServer: copyGroup failed, grp = '%s', exception = '%s', continue"%(group,e))
                continue

    def getStats(self):
        """Get the statistics object."""
        return self.stats

print("start class nntpsucka done")
########################################################################
print("start class OptConf")

class OptConf(ConfigParser.ConfigParser):
    """ConfigParser with get that supports default values"""

    def __init__(self, defaults=None, sections=[]):
        ConfigParser.ConfigParser.__init__(self, defaults)
        for s in sections:
            if not self.has_section(s):
                self.add_section(s)

    def getWithDefault(self, section, option, default, raw=False, vars=None):
        """returns the configuration entry, or the ``default'' argument"""
        rv=default
        try:
            rv=self.get(section, option, raw, vars)
        except ConfigParser.NoSectionError:
            pass
        except ConfigParser.NoOptionError:
            pass
        return rv

class Timeout:
    """This is an exception that's raised when the alarm handler fires."""
    pass

print("start class OptConf done")
########################################################################
print("start defs")

def alarmHandler(sig, frame):
    """Do nothing but raise a timeout."""
    raise Timeout

def getIgnoreList(fn):
    log=logging.getLogger("nntpsucka")
    try:
        
        if not os.path.isfile(fn):
            log.warn("def getIgnoreList: '%s' not found "%(fn))
            return None
        log.info("def getIgnoreList: loading '%s' "%(fn))
        rv=[]
        f=open(fn)
        lines = 0
        for l in f.readlines():
            l=l.strip()
            if l == '':
                log.warn("def getIgnoreList: error, empty line %d"%lines)
                return None
            if l in rv:
                continue
            rv.append(re.compile(l))
            log.debug("def getIgnoreList: add '%s'"%(l))
            lines += 1
        if len(rv) > 0:
            log.info("def getIgnoreList: loaded %d"%(len(rv)))
            return rv
        else:
            return None
    except Exception as e:
        log.warn("def getGlobalFilterList: failed, exception = '%s'"%(e))

def getForcedList(fn):
    log=logging.getLogger("nntpsucka")
    try:
        if not os.path.isfile(fn):
            log.warn("def getForcedList: '%s' not found "%(fn))
            return None
        log.debug("def getForcedList: loading '%s' "%(fn))
        rv=[]
        f=open(fn)
        lines = 0
        for l in f.readlines():
            l=l.strip()
            if l == '':
                log.warn("def getForcedList: error, empty line %d"%lines)
                return None
            if l in rv:
                break
            rv.append(re.compile(l))
            log.debug("def getForcedList '%s': add '%s'"%(fn,l))
            lines += 1
        if len(rv) > 0:
            log.info("def getForcedList: loaded %d"%(len(rv)))
            return rv
        else:
            return None
    except Exception as e:
        log.warn("def getForcedList: failed, exception = '%s'"%(e))
        sys.exit(1)

def getDoneList(fn):
    log=logging.getLogger("nntpsucka")
    try:
        if not os.path.isfile(fn):
            log.warn("def getDoneList: '%s' not found "%(fn))
            return None
        log.info("def getDoneList: loading '%s' "%(fn))
        rv=[]
        f=open(fn)
        lines = 0
        for l in f.readlines():
            l=l.strip()
            if l == '':
                log.warn("def getDoneList: error, empty line %d"%lines)
                return None
            if l in rv:
                break
            #rv.append(re.compile(l))
            log.debug("def getDoneList: add ignore '%s'"%(l))
            rv.append(l)
            lines += 1
        if len(rv) > 0:
            log.info("def getDoneList: loaded %d"%(len(rv)))
            return rv
        else:
            return None
    except Exception as e:
        log.warn("def getDoneList: failed, exception = '%s'"%(e))

def getGlobalFilterList(fn):
    log=logging.getLogger("nntpsucka")
    try:
        if not os.path.isfile(fn):
            log.warn("def getGlobalFilterList: '%s' not found "%(fn))
            return None
        log.info("def getGlobalFilterList: loading '%s' "%(fn))
        rv=[]
        f=open(fn)
        lines = 0
        for l in f.readlines():
            l=l.strip()
            if l == '':
                log.warn("def getGlobalFilterList: error, empty line %d"%lines)
                return None
            if l in rv:
                break
            rv.append(re.compile(l))
            log.debug("def getGlobalFilterList: add '%s'"%(l))
            lines += 1
        if len(rv) > 0:
            log.info("def getGlobalFilterList: loaded %d"%(len(rv)))
            return rv
        else:
            return None    
    except Exception as e:
        log.warn("def getGlobalFilterList: failed, exception = '%s'"%(e))

def writetoDoneList(group):
    if CONFIG['running_workers'] == 0:
            return True
    log=logging.getLogger("nntpsucka")
    try:
        fn = CONFIG['file_doneList']
        if fn != None:
            log.info("def writetoDoneList: fn='%s', group='%s'"%(fn,group))
            fp = open(fn, "a")
            fp.write(group+'\n')
            fp.close()
            log.debug("def writetoDoneList: fn='%s', group='%s', regex = 'False' done"%(fn,group))
    except Exception as e:
        log.warn("def writetoDoneList failed, exception = '%s', fn='%s', group='%s'"%(e,fn,group))
        
def writetoBadGroupsList(group):
    if CONFIG['running_workers'] == 0:
            return True
    log=logging.getLogger("nntpsucka")
    try:
        fn = "list.BadGroups.txt"
        if fn != None:
            log.info("def writetoBadList: fn='%s', group='%s'"%(fn,group))
            fp = open(fn, "a")
            fp.write(group+'\n')
            fp.close()
            log.debug("def writetoBadList: fn='%s', group='%s', regex = 'False' done"%(fn,group))
    except Exception as e:
        log.warn("def writetoBadList failed, exception = '%s', fn='%s', group='%s'"%(e,fn,group))

def writetoMessageList(fn,group,l):
    log=logging.getLogger("nntpsucka")
    try:
        if fn != None:
            fp = open(fn, "a")
            log.info("def writetoMessageList: fn='%s', group='%s', lent='%s', writing"%(fn,group,len(l)))
            for i in l:
                ws = '%d %s' % (int(i[0]),i[1])
                fp.write(ws+'\n')
            fp.close()
            log.info("def writetoMessageList: fn='%s', group='%s', lent='%s', done"%(fn,group,len(l)))
            return True
        return False
    except Exception as e:
        log.warn("def writetoMessageList failed, exception = '%s', fn='%s', group='%s'"%(e,fn,group))

def tryfixarticle(which,pgrp):
    fn = "%s/.art%s" % (pgrp,which)

def writebadarticle(group,which,messid,pgrp):
    log=logging.getLogger("nntpsucka")
    fn = "bad.%s" % group
    try:
        if fn != None:
            fp = open(fn, "a")
            text = "group=%s num=%s messid=%s pgrp=%s/.art%s" % (group,which,messid,pgrp,which)
            log.info("def writebadarticles: fn='%s' writing: '%s'"%(fn,text))
            fp.write(text+'\n')
            #for i in l:
            #    ws = '%d %s' % (int(i[0]),i[1])
            #    fp.write(ws+'\n')
            fp.close()
            log.debug("def writebadarticles: fn='%s' done"%(fn))
            return True
        return False
    except Exception as e:
        log.warn("def writebadarticles failed, exception = '%s', fn='%s', group='%s'"%(e,fn,group))

def readMessagesList(fn):
    log=logging.getLogger("nntpsucka")
    rv=[]
    try:
        if os.path.isfile(fn):
            f=open(fn)
            for l in f.readlines():
                if l == '':
                    break
                rv.append(l.split())
            log.info("def readMessagesList: got %d msg-ids from file '%s'"%(len(rv),fn))
    except Exception as e:
        log.warn("def readMessagesList: failed, exception = '%s'"%(e))
    return rv

def clean_REASON(reason):
    strreason = str(reason)
    if "timeout" in strreason:
        reason = "timeout"
    elif "refused" in strreason:
        reason = "refused"
    elif "cleanly" in strreason:
        reason = "cleanly"
    elif "unclean" in strreason:
        reason = "unclean"
    else:
        reason = "error unknown reason: '%s'" % reason
    return reason

def connectionMaker(conf, which):
    log=logging.getLogger("nntpsucka")
    try:
        connServer=conf.get("servers", which)
        connUser=None
        connPass=None
        connPort=119
        num_conn = 1
        if conf.has_section(connServer):
            connUser=conf.getWithDefault(connServer, "username", None)
            connPass=conf.getWithDefault(connServer, "password", None)
            connPort=conf.getint(connServer, "port")

        def f():
            return NNTPClient(connServer, port=connPort, user=connUser, password=connPass)

        return f
    except Exception as e:
        log.warn("def connectionMaker: failed, exception = '%s'"%(e))

print("start defs done")
########################################################################
print("start def main()")

def main():
    global CONFIG
    conf=OptConf(CONF_DEFAULTS, CONF_SECTIONS)
    conf.read(sys.argv[1])

    # Lock to make sure only one is running at a time.
    lock=pidlock.PidLock(conf.get("misc", "pidfile"))

    # How long do we wait for startup?
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(120)
    # And how long will we wait for the actual processing?
    TIMEOUT = 86400

    # Validate there's a config file
    if len(sys.argv) < 2:
        sys.stderr.write("Usage:  " + sys.argv[0] + " configFile\n")
        sys.exit(1)

    # Configure logging.
    logging.config.fileConfig(sys.argv[1])
    
    filterList = conf.getWithDefault("misc", "filterList", None)
    globalfilterList = conf.getWithDefault("misc", "globalfilterList", None)
    forcedList = conf.getWithDefault("misc", "forcedList", None)
    useignore = conf.getboolean("misc", "useIgnore")
    doneList = conf.getWithDefault("misc", "doneList", None)
    workers = conf.getint("misc", "workers")
    
    fromFactory = connectionMaker(conf, "from")
    toFactory = connectionMaker(conf, "to")
    
    modeSelect = conf.getWithDefault("misc", "mode", None)
    if modeSelect == "ihave":
        CONFIG['mode'] = "ihave"
    elif modeSelect == "ihave2":
        CONFIG['mode'] = "ihave2"
    elif modeSelect == "post":
        CONFIG['mode'] = "post"
    elif modeSelect == "mbox":
        CONFIG['mode'] = "mbox"
    elif modeSelect == "reader1":
        CONFIG['mode'] = "reader1"
    else:
        CONFIG['mode'] = "reader"
        
    sucka=None
    # Mark the start time
    start=time.time()
    sys.stdout.write("def main()")
    try:
        CONFIG['file_doneList'] = doneList
        CONFIG['useIgnore'] = useignore
        
        if filterList is not None:
            CONFIG['filterList'] = getIgnoreList(filterList)
        else:
            CONFIG['filterList'] = None
                    
        if globalfilterList is not None:
            CONFIG['globalfilterList'] = getGlobalFilterList(globalfilterList)
        else:
            CONFIG['globalfilterList'] = None
        
        if forcedList is not None:
            CONFIG['forcedList'] = getForcedList(forcedList)
        else:
            CONFIG['forcedList'] = None
        
        if doneList is not None:
            print("load donelist file=%s"%(doneList))
            CONFIG['doneList'] = getDoneList(doneList)
        else:
            CONFIG['doneList'] = None
       
        signal.alarm(120)
        print("start sucka:copyServer")
        sucka=NNTPSucka(fromFactory, toFactory, config=conf)
        signal.alarm(TIMEOUT)
        sucka.copyServer()
    except Timeout:
        sys.stderr.write("Took too long.\n")
        sys.stdout.write("Took too long.\n")
        sys.exit(1)

    # Mark the stop time
    stop=time.time()
    if sucka:
        # Log the stats
        log=logging.getLogger("nntpsucka")
        log.info(sucka.getStats())
        if CONFIG['running_workers'] == workers:
            log.info("Total time spent:  " + str(stop-start) + "s (good exit)")
            sys.exit(0)
        else:
            log.info("Total time spent:  " + str(stop-start) + "s (unclean exit)")
            sys.exit(1)

print("start def main done")
if __name__ == '__main__':
    try:
        print("launch main()")
        main()
    except pidlock.AlreadyLockedException, ale:
        sys.stderr.write("Already running:  " + str(ale[0]) + "\n")
