#!/usr/bin/env python

# This script takes extracted site files and inserts them into the database.

from __future__ import with_statement

import sys
import os
import xml.sax
from datetime import datetime
import platform
import re

from sqlobject import *
from pysolr import Solr

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

is_jython = 'Java' in platform.system()
script_dir = os.path.dirname(sys.argv[0])

# MODELS
# use UnicodeCol instead of StringCol; StringCol defaults to ascii when encoding
# is unspecified, as it is with Jython zxJDBC.
class Site(SQLObject):
    name = UnicodeCol()
    desc = UnicodeCol()

class Badge(SQLObject):
    sourceId = IntCol()
    site = ForeignKey('Site', cascade=True)
    userId = IntCol()
    name = UnicodeCol()
    date = DateTimeCol()

class Comment(SQLObject):
    sourceId = IntCol()
    site = ForeignKey('Site', cascade=True)
    postId = IntCol()
    score = IntCol()
    text = UnicodeCol()
    creationDate = DateTimeCol()
    userId = IntCol()
    
    json_fields = [ 'id', 'score', 'text', 'creationDate', 'userId' ]

class User(SQLObject):
    sourceId = IntCol()
    site = ForeignKey('Site', cascade=True)
    reputation = IntCol()
    creationDate = DateTimeCol()
    displayName = UnicodeCol()
    emailHash = UnicodeCol()
    lastAccessDate = DateTimeCol()
    websiteUrl = UnicodeCol()
    location = UnicodeCol()
    age = IntCol()
    aboutMe = UnicodeCol()
    views = IntCol()
    upVotes = IntCol()
    downVotes = IntCol()

# SAX HANDLERS
# Jython can't handle the %f format specifier
if is_jython:
    ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
else:
    ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

class BaseContentHandler(xml.sax.ContentHandler):
    """
    Base content handler.
    """
    def __init__(self, site, obj_class):
        self.site = site
        self.obj_class = obj_class
        self.cur_props = None
        self.row_count = 0
    
    def endElement(self, name):
        if name != 'row':
            return
        
        if not self.cur_props:
            return
        
        # we want to count failed rows as well as successful ones as this is
        # a count of rows processed.
        self.row_count += 1
        
        # the cur_props is now complete. Save it.
        try:
            # the object is automatically saved to the database on creation
            self.obj_class(**self.cur_props)
        except Exception, e:
            # could not insert this, so ignore the row
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not insert the row ' + repr(self.cur_props))
        
        self.cur_props = None
        
        if self.row_count % 1000 == 0:
            print('[badge]\t\tProcessed %d rows.' % (self.row_count))

class BadgeContentHandler(BaseContentHandler):
    """
    Parses the string -
    
    <row Id="15" UserId="6" Name="Supporter" Date="2010-05-19T21:57:31.000" />
    """
    def __init__(self, site):
        BaseContentHandler.__init__(self, site, Badge)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'site' : self.site }
            # this hack to get the Id attr is needed due to Jython bug #1768
            d['sourceId'] = is_jython and int(attrs._attrs.getValue('Id')) or int(attrs['Id'])
            d['userId'] = int(attrs.get('UserId', 0))
            d['name'] = attrs.get('Name', '')
            d['date'] = datetime.strptime(attrs.get('Date'), ISO_DATE_FORMAT)
        except Exception, e:
            # could not parse this, so ignore the row completely
            self.cur_props = None
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not parse the row ' + repr(attrs))

class CommentContentHandler(BaseContentHandler):
    """
    Parses the string -
    
    <row Id="6" PostId="12" Score="1" Text="Just as an addition, if you are \
            going to test your library, don't use a key (that is what no key \
            is for). However, make sure the final product demands and API \
            key. " CreationDate="2010-05-19T23:48:05.680" UserId="23" />

    """
    def __init__(self, site):
        BaseContentHandler.__init__(self, site, Comment)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'site' : self.site }
            # this hack to get the Id attr is needed due to Jython bug #1768
            d['sourceId'] = is_jython and int(attrs._attrs.getValue('Id')) or int(attrs['Id'])
            d['postId'] = int(attrs.get('PostId', 0))
            d['score'] = int(attrs.get('Score', 0))
            d['text'] = attrs.get('Text', '')
            d['creationDate'] = datetime.strptime(attrs.get('CreationDate'), ISO_DATE_FORMAT)
            d['userId'] = int(attrs.get('UserId', 0))
            
        except Exception, e:
            # could not parse this, so ignore the row completely
            self.cur_props = None
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not parse the row ' + repr(attrs))

class UserContentHandler(BaseContentHandler):
    """
    Parses the string -
    
     <row Id="1" Reputation="176" CreationDate="2010-05-19T21:30:04.837" \
             DisplayName="Geoff Dalgas" \
             EmailHash="b437f461b3fd27387c5d8ab47a293d35" \
             LastAccessDate="2011-04-17T04:58:28.830" \
             WebsiteUrl="http://stackoverflow.com" Location="Corvallis, OR" \
             Age="33" AboutMe="&lt;p&gt;Developer on the StackOverflow team.  \
             Find me on&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;a \
             href=&quot;http://www.twitter.com/SuperDalgas&quot; \
             rel=&quot;nofollow&quot;&gt;Twitter&lt;/a&gt;&#xA;&lt;br&gt;&lt;br\
             &gt;&#xA;&lt;a href=&quot;http://blog.stackoverflow.com/2009/05/\
             welcome-stack-overflow-valued-associate-00003/&quot; rel=&quot;\
             nofollow&quot;&gt;Stack Overflow Valued Associate #00003&lt;/a&gt;\
             &lt;/p&gt;&#xA;" Views="52" UpVotes="11" DownVotes="1" />

    """
    def __init__(self, site):
        BaseContentHandler.__init__(self, site, User)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'site' : site }
            # this hack to get the Id attr is needed due to Jython bug #1768
            d['sourceId'] = is_jython and int(attrs._attrs.getValue('Id')) or int(attrs['Id'])
            d['reputation'] = int(attrs.get('Reputation', 0))
            d['creationDate'] = datetime.strptime(attrs.get('CreationDate'), ISO_DATE_FORMAT)
            d['displayName'] = attrs.get('DisplayName', '')
            d['emailHash'] = attrs.get('EmailHash', '')
            d['lastAccessDate'] = datetime.strptime(attrs.get('LastAccessDate'), ISO_DATE_FORMAT)
            d['websiteUrl'] = attrs.get('WebsiteUrl', '')
            d['location'] = attrs.get('Location', '')
            d['age'] = int(attrs.get('Age', 0))
            d['aboutMe'] = attrs.get('AboutMe', '')
            d['views'] = int(attrs.get('Views', 0))
            d['upVotes'] = int(attrs.get('UpVotes', 0))
            d['downVotes'] = int(attrs.get('DownVotes', 0))
            
        except Exception, e:
            # could not parse this, so ignore the row completely
            self.cur_props = None
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not parse the row ' + repr(attrs))

class PostContentHandler(xml.sax.ContentHandler):
    """
    Parses the string -
    
    <row Id="1" PostTypeId="1" AcceptedAnswerId="509" \
            CreationDate="2009-04-30T06:49:01.807" Score="13" ViewCount="820" \
            Body="&lt;p&gt;Our nightly full (and periodic differential) \
            backups are becoming quite large, due mostly to the amount of \
            indexes on our tables; roughly half the backup size is comprised \
            of indexes.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;We're using the \
            &lt;strong&gt;Simple&lt;/strong&gt; recovery model for our \
            backups.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Is there any way, through \
            using &lt;code&gt;FileGroups&lt;/code&gt; or some other \
            file-partitioning method, to &lt;strong&gt;exclude&lt;/strong&gt; \
            indexes from the backups?&lt;/p&gt;&#xA;&#xA;&lt;p&gt;It would be \
            nice if this could be extended to full-text catalogs, as \
            well.&lt;/p&gt;&#xA;" OwnerUserId="3" LastEditorUserId="919" \
            LastEditorDisplayName="" LastEditDate="2009-05-04T02:11:16.667" \
            LastActivityDate="2009-05-10T15:22:39.707" Title="How to exclude \
            indexes from backups in SQL Server 2008" \
            Tags="&lt;sql-server&gt;&lt;backup&gt;&lt;sql-server-2008&gt;&lt;indexes&gt;" \
            AnswerCount="3" CommentCount="1" FavoriteCount="3" />

    """
    TAGS_RE = re.compile(u'&lt;([\w\d\-]+)&gt;')
    
    def __init__(self, site):
        self.site = site
        self.unfinished_questions = { }
        self.cur_props = None
        self.row_count = 0
    
    def json_default_handler(self, obj):
        # for date object handling
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { }
            # this hack to get the Id attr is needed due to Jython bug #1768
            d['id'] = is_jython and int(attrs._attrs.getValue('Id')) or int(attrs['Id'])
            
            if attrs['PostTypeId'] == '2':
                # I am an answer.
                d['parentId'] = int(attrs['ParentId'])
            elif attrs['PostTypeId'] == '1':
                # I am a question.
                d['answers'] = [ ]
                d['answerCount'] = int(attrs.get('AnswerCount', 0))
                d['viewCount'] = int(attrs.get('ViewCount', 0))
            else:
                raise ValueError('Unknown PostTypeId [%s] for row ID [%s]' % (attrs.get('PostTypeId', -1), attrs.get('Id', -1)))
            
            if 'AcceptedAnswerId' in attrs:
                d['acceptedAnswerId'] = int(attrs.get('AcceptedAnswerId', 0))
            d['creationDate'] = datetime.strptime(attrs.get('CreationDate'), ISO_DATE_FORMAT)
            d['score'] = int(attrs.get('Score', 0))
            d['body'] = attrs.get('Body', '')
            d['ownerUserId'] = int(attrs.get('OwnerUserId', 0))
            if 'LastEditorUserId' in attrs:
                d['lastEditorUserId'] = int(attrs.get('LastEditorUserId', ''))
            if 'LastEditDate' in attrs:
                d['lastEditDate'] = datetime.strptime(attrs.get('LastEditDate'), ISO_DATE_FORMAT)
            d['lastActivityDate'] = datetime.strptime(attrs.get('LastActivityDate'), ISO_DATE_FORMAT)
            if 'CommunityOwnedDate' in attrs:
                d['communityOwnedDate'] = datetime.strptime(attrs.get('CommunityOwnedDate'), ISO_DATE_FORMAT)
            if 'ClosedDate' in attrs:
                d['closedDate'] = datetime.strptime(attrs.get('ClosedDate'), ISO_DATE_FORMAT)
            d['title'] = attrs.get('Title', '')
            if 'Tags' in attrs:
                d['tags'] = attrs.get('Tags', '')
            d['commentCount'] = int(attrs.get('CommentCount', 0))
            d['favoriteCount'] = int(attrs.get('FavoriteCount', 0))
            d['comments'] = [ ]
            
            
        except Exception, e:
            # could not parse this, so ignore the row completely
            self.cur_props = None
            print('Exception: ' + str(e))
            # TODO: enable these in verbose/debug output mode
            #import traceback
            #traceback.print_exc()
            #print('Could not parse the row ' + repr(dict([(k,attrs[k]) for k in attrs.getNames()])))
    
    def endElement(self, name):
        if name != 'row':
            return
        
        if not self.cur_props:
            return
        
        # we want to count failed rows as well as successful ones as this is
        # a count of rows processed.
        self.row_count += 1
        
        try:
            d = self.cur_props
            
            # find, convert to JSON and attach any comments for this question
            comments = Comment.select(AND(Comment.q.site == self.site,
                                          Comment.q.postId == int(d['id'])))
            for comment in comments:
                c = { }
                for f in Comment.json_fields:
                    c[f] = getattr(comment, f)
                d['comments'].append(c)
            
            if len(d['comments']) != d['commentCount']:
                print('Post ID [%s] expected to have %d comments, but got %d instead. Ignoring inconsistency.' % (d['id'], d['commentCount'], len(d['comments'])))
            
            # the cur_props is now complete. Stash it away until question is complete.
            if d.has_key('parentId'):
                # this is an answer.
                if not self.unfinished_questions.has_key(d['parentId']):
                    print('lookup keys: ' + repr(self.unfinished_questions.keys()))
                    raise ValueError("This answer's [ID# %s] question [ID# %s] has not been processed yet. Incorrect order in XML? Ignoring answer." % (d['id'], d['parentId']))
                else:
                    self.unfinished_questions[d['parentId']]['answers'].append(d)
            else:
                # this is a question.
                if self.unfinished_questions.has_key(d['id']):
                    # this should not occur; duplicate question id.
                    raise ValueError('Question ID [%s] already exists.\nThis title: %s\nDuplicate title:%s\nIgnoring duplicate.' %
                                     (d['id'], d['title'], self.unfinished_questions[d['id']]['title']))
                else:
                    self.unfinished_questions[d['id']] = d
            
        except Exception, e:
            # could not insert this, so ignore the row
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not process the row ' + repr(self.cur_props))
        
        self.cur_props = None
        
        if self.row_count % 1000 == 0:
            print('\tProcessed %d rows.' % (self.row_count))
        
        self.commit_finished_questions()
    
    def commit_finished_questions(self):
        # check if any questions are now complete (answerCount=len(answers))
        finished_question_ids = [ ]
        for id, q in self.unfinished_questions.items():
            if len(q['answers']) >= q['answerCount']:
                if len(q['answers']) > q['answerCount']:
                    print('Question ID [%s] expected to have %d answers, but got %d instead. Ignoring inconsistency.' % (q['id'], q['answerCount'], len(q['answers'])))
             
                try:
                    # question is complete, store it.
                    self.commit_question(q)
        
                except Exception, e:
                    # could not serialise and insert this question, so ignore it
                    print('Exception: ' + str(e))
                    import traceback
                    traceback.print_exc()
                    print('Could not process the completed question ' + repr(q))
            
                finally:
                    finished_question_ids.append(id)
        
        # remove any finished questions from the unfinished list
        for id in finished_question_ids:
            self.unfinished_questions.pop(id)
    
    def commit_question(self, q):
        """
        Massages and serialises the question object so it can be inserted into
        the search index in the form that we want.
        
        Things this does -
        * creates the 'text' field for the search index that contains all the
          text of the question (title, question, answers and all comments).
        * serialises answers to JSON
        * creates dict that maps to the search index document schema
        * remove unwanted attributes from the q object and serialise question to
          JSON
        * add question JSON to document
        * commit document to search index.
        """
        doc = { }
        
        # create the text field contents
        search_text = [ ]
        # question bits
        search_text.append(q['title'])
        search_text.append(q['body'])
        for c in q['comments']:
            search_text.append(c['text'])
        
        # answer bits
        for a in q['answers']:
            search_text.append(a['body'])
            for c in a['comments']:
                search_text.append(c['text'])
        
        search_text = ' '.join(search_text)
        doc['text'] = search_text
        
        # serialise answers to JSON
        doc['answer-json'] = [ json.dumps(a, default=self.json_default_handler) for a in q['answers'] ]
        
        # map other fields to search index doc
        doc['id'] = str(q['id'])
        doc['siteName'] = self.site.name
        doc['creationDate'] = q['creationDate']
        doc['score'] = q['score']
        doc['viewCount'] = q['viewCount']
        doc['title'] = q['title']
        doc['ownerUserId'] = q['ownerUserId']
        if 'lastEditorUserId' in q:
            doc['lastEditorUserId'] = q['lastEditorUserId']
        doc['lastActivityDate'] = q['lastActivityDate']
        if 'communityOwnedDate' in q:
            doc['communityOwnedDate'] = q['communityOwnedDate']
        if 'closedDate' in q:
            doc['closedDate'] = q['closedDate']
        if 'tags' in q:
            # parse tags into a list
            doc['tags'] = PostContentHandler.TAGS_RE.findall(q['tags'])
        
        # serialise question to JSON (the q object has cruft we don't want)
        question_obj = { }
        question_obj['id'] = q['id']
        if 'acceptedAnswerId' in q:
            question_obj['acceptedAnswerId'] = q['acceptedAnswerId']
        question_obj['creationDate'] = q['creationDate']
        question_obj['score'] = q['score']
        question_obj['viewCount'] = q['viewCount']
        question_obj['body'] = q['body']
        question_obj['ownerUserId'] = q['ownerUserId']
        if 'lastEditorUserId' in q:
            question_obj['lastEditorUserId'] = q['lastEditorUserId']
        if 'LastEditDate' in q:
            question_obj['lastEditDate'] = q['lastEditDate']
        question_obj['lastActivityDate'] = q['lastActivityDate']
        if 'communityOwnedDate' in q:
            question_obj['communityOwnedDate'] = q['communityOwnedDate']
        if 'closedDate' in q:
            question_obj['closedDate'] = q['closedDate']
        question_obj['title'] = q['title']
        if 'tags' in q:
            question_obj['tags'] = q['tags']
        question_obj['favoriteCount'] = q['favoriteCount']
        question_obj['comments'] = q['comments']
        
        doc['question-json'] = json.dumps(question_obj, default=self.json_default_handler)
        
        solr.add([ doc ])
    
    def commit_all_questions(self):
        """
        Commits all questions, regardless of whether they're completed or not.
        
        Should be called after all XML has been parsed.
        """
        for id,q in self.unfinished_questions.items():
            print('Question [ID# %d] was expected to have %d answers, but got %d instead. Ignoring inconsistency.' % (q['id'], q['answerCount'], len(q['answers'])))

            try:
                # question is complete, store it.
                self.commit_question(q)
    
            except Exception, e:
                # could not serialise and insert this question, so ignore it
                print('Exception: ' + str(e))
                import traceback
                traceback.print_exc()
                print('Could not process the question ' + repr(q))

        # we're committing all questions, so nothing is now unfinished
        self.unfinished_questions.clear()

# MAIN METHOD
if len(sys.argv) != 2:
    print('One argument is expected - the path to the extracted XML files.')
    sys.exit(1)

xml_root = sys.argv[1]
print('Using the XML root path: ' + xml_root + '\n')

if not os.path.exists(xml_root):
    print('The given XML root path does not exist.')
    sys.exit(1)

db_path = os.path.abspath(os.path.join(script_dir, '../../data/stackdump.sqlite'))

# connect to the database
print('Connecting to the database...')
if is_jython:
    conn_str = 'jython_sqlite://' + db_path
else: # assume cPython
    conn_str = 'sqlite://' + db_path
sqlhub.processConnection = connectionForURI(conn_str)
#sqlhub.processConnection = connectionForURI('jython_sqlite://:memory:')
print('Connected.\n')

# connect to solr
print('Connecting to solr...')
solr = Solr("http://localhost:8983/solr/")
print('Connected.\n')

# ensure required tables exist
print("Creating tables if they don't exist...")
Site.createTable(ifNotExists=True)
Badge.createTable(ifNotExists=True)
Comment.createTable(ifNotExists=True)
User.createTable(ifNotExists=True)
print('Created.\n')

# SITE NAME
# get the site name from the first line of readme.txt. This could be fragile.
with open(os.path.join(xml_root, 'readme.txt')) as f:
    site_desc = f.readline().strip()

# assume if there's a colon in the name, the name part is before, and the date
# part is after.
if ':' in site_desc:
    site_name, site_date = site_desc.split(':')
else:
    site_name = site_desc
    site_date = ''

print('Site name is %s\n' % site_name)

# check if site is already in database; if so, purge the data.
sites = Site.select(Site.q.name==site_name)
# the site really shouldn't exist more than once, but just in case
for site in sites:
    print('Deleting site "%s" from the database... ' % site.desc)
    sys.stdout.flush()
    Site.delete(site.id) # the relationship cascades, so other rows will be deleted
    print('Deleted.\n')

print('Deleting site "%s" from the solr... ' % site_desc)
solr.delete(q='siteName:"%s"' % site_name)
print('Deleted.\n')

# create a new Site
site = Site(name=site_name, desc=site_desc)

# BADGES
print('[badge] PARSING BADGES...')
sqlhub.threadConnection = sqlhub.processConnection.transaction()
xml_path = os.path.join(xml_root, 'badges.xml')
print('[badge] start parsing badges.xml...')
handler = BadgeContentHandler(site)
xml.sax.parse(xml_path, handler)
sqlhub.threadConnection.commit(close=True)
print('[badge]\t\tProcessed %d rows.' % (handler.row_count))
print('[badge] FINISHED PARSING BADGES.\n')

# COMMENTS
print('[comment] PARSING COMMENTS...')
sqlhub.threadConnection = sqlhub.processConnection.transaction()
xml_path = os.path.join(xml_root, 'comments.xml')
print('[comment] start parsing comments.xml...')
handler = CommentContentHandler(site)
xml.sax.parse(xml_path, handler)
sqlhub.threadConnection.commit(close=True)
print('[comment]\tProcessed %d rows.' % (handler.row_count))
print('[comment] FINISHED PARSING COMMENTS.\n')

# USERS
print('[user] PARSING USERS...')
sqlhub.threadConnection = sqlhub.processConnection.transaction()
xml_path = os.path.join(xml_root, 'users.xml')
print('[user] start parsing users.xml...')
handler = UserContentHandler(site)
xml.sax.parse(xml_path, handler)
sqlhub.threadConnection.commit(close=True)
print('[user]\t\tProcessed %d rows.' % (handler.row_count))
print('[user] FINISHED PARSING USERS.\n')

# POSTS
print('[post] PARSING POSTS...')
sqlhub.threadConnection = sqlhub.processConnection.transaction()
xml_path = os.path.join(xml_root, 'posts.xml')
print('[post] start parsing posts.xml...')
handler = PostContentHandler(site)
xml.sax.parse(xml_path, handler)
handler.commit_all_questions()
sqlhub.threadConnection.commit(close=True)
print('[post]\tProcessed %d rows.' % (handler.row_count))

print('[post] FINISHED PARSING POSTS.\n')

# TODO: delete comments?
