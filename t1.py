#!/usr/bin/env python
import feedparser
import re
import os
from stat import *
import urllib2
import gzip
import zlib
import mysql.connector
from mysql.connector import errorcode
from config import *
import socket
import boto
from datetime import date, datetime, timedelta
import sys

regexcompiled = None


d = feedparser.parse('https://community.emc.com/community/feeds/allcontent?community=1&showProjectContent=false&recursive=true')

def getnexturl(plink):
    regexcompiled = re.compile('.*thread/')
    threadnumber = regexcompiled.sub('',plink)
    myurl = 'https://community.emc.com/community/feeds/messages?thread='
    myurl = myurl + threadnumber
    return myurl

def blob_exists(fname):
    try:
        mode = os.stat(fname).st_mode
        if S_ISREG(mode):
            rc = True
        else:
            rc = False
    except OSError:
        rc = False
    return rc

def write_blob_sql_update(guid,type,locator,hostname):
    # tell the metadata where we're writing the blob

    # Put a record in this table:
    # create TABLE IF NOT EXISTS postlocator (
    # postnum integer(11) NOT NULL,
    # posttype varchar(8) NOT NULL,
    # filename varchar(255) UNIQUE,
    # hostname varchar(64),
    # PRIMARY KEY (postnum));

    # Get the postnum using the guid
    cnx = dbopen()  # How expensive is this?  Should we maintain state?
    curs = cnx.cursor()
    sel = "SELECT postnum from post where guid = '%s'" % guid
    curs.execute((sel))
    postnum = curs.fetchone()
    if postnum is None:
        print 'error - should never happen.  Trying to update locator with invalid postnum'
        rc = False
    else:
        upd = ("INSERT INTO postlocator "
               "(postnum,posttype,filename,hostname) "
               "VALUES(%s,%s,%s,%s)")
        vals = (postnum[0],type,locator,hostname)
        cursor.execute(upd,vals)
        cnx.commit()
        rc = True
    curs.close()
    cnx.close()
    return rc


def write_blob(myblob,guid,persistencetype):
    r = re.compile('/')     # Remove the slashes so the GUID is a valid Linux file

    outdir = os.getcwd()+'/output/'
    blobkey = outdir + r.sub('%',guid) + '.gz'

    myblob = zlib.compress(myblob)

    if persistencetype == 'file':
    # Change / to % and make this a filename
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        f = open (blobkey, 'w')
        f.write(myblob)
        f.close()
        rc = True
    elif persistencetype == 's3':
        bucketkey.key = blobkey
        bucketkey.set_contents_from_string(myblob)
        rc = True
    else:
        print 'not implemented'
        rc = False
    write_blob_sql_update(guid,persistencetype,blobkey,hostname)
    return rc

def get_blob(myurl):
    response = urllib2.urlopen(myurl)
    html = response.read()
    print "HTML Length:",len(html)
    return html

def dbopen():
    try:
        cnx = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASS, host=MYSQL_HOST,database=MYSQL_DATABASE)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists")
        else:
            print(err)
        cnx = False
    return cnx

def dbclose(c):
    c.close()

def checknew(myguid):
    # Ask My SQL if we've already seen this GUID.
    cnx = dbopen()  # How expensive is this?  Should we maintain state?
    curstmp = cnx.cursor()
    sel = "SELECT postnum from post where guid = '%s'" % myguid
    curstmp.execute((sel))
    Row = curstmp.fetchone()
    if Row is None:
        # New Post - not in metadata
        rc = True
    else:
        rc = False
    curstmp.close()
    cnx.close()
    return rc

# Globals
hostname = ""

if __name__ == "__main__":
    hostname = socket.gethostname()
    mydb = dbopen()
    persist = "file"

    if not mydb:
        print 'failed to open mysql\nexiting'
        exit()

    if persist == "s3":
        s3 = boto.connect_s3()
        bucket_name = "c1d2"
        from boto.s3.key import Key
        bucket = s3.get_bucket(bucket_name)
        bucketkey = Key(bucket)

    cursor = mydb.cursor()
    add_post = ("INSERT INTO post "
                "(guid,title,description,url,discoverdate) "
                "VALUES(%s,%s,%s,%s,%s)")
    for post in d['entries']:
        discoverdate = datetime.now().date()
        if checknew(post.guid):
            print 'discovered new post:',len(post.title),post.title,'---'
            # print len(post.link),post.link
            # print "   ",post.link
            # print "   ",len(post.description),post.description
            try:
                pdata = (post.guid.encode('ascii'),post.title,post.description,post.link,discoverdate)
                cursor.execute(add_post,pdata)
                mydb.commit()
                blob = get_blob(post.link)
                if persist == "file":
                    write_blob(blob,post.guid,'file')
                elif persist == "s3":
                    write_blob(blob,post.guid,'s3')
                else:
                    print "no persistence for page content"
                print '\n___________________\n'
            except:
                e = sys.exc_info() [0]
                print 'exception handling add_post() - {}'.format(e)
        else:
            if False: print "duplicate entry %s skipped" % post.title

    cursor.close()
    dbclose(mydb)

