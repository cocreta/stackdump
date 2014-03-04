#!/usr/bin/env python

# This script takes extracted site files and inserts them into the database.

from __future__ import with_statement

import sys
import os
import time
import xml.sax
from datetime import datetime
import re
import urllib2
import socket
import tempfile
import traceback
from optparse import OptionParser
from xml.etree import ElementTree

from sqlobject import sqlhub, connectionForURI, AND, IN, SQLObject, \
    UnicodeCol, DateTimeCol, IntCol, DatabaseIndex, dbconnection
from sqlobject.sqlbuilder import Delete, Insert
from sqlobject.styles import DefaultStyle
from pysolr import Solr, SolrError

from stackdump.models import Site, Badge, User
from stackdump import settings

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

script_dir = os.path.dirname(sys.argv[0])

# SAX HANDLERS
ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

class BaseContentHandler(xml.sax.ContentHandler):
    """
    Base content handler.
    """
    def __init__(self, conn, site, obj_class):
        self.conn = conn
        self.site = site
        self.obj_class = obj_class
        self.cur_props = None
        self.row_count = 0
        self.db_style = DefaultStyle()
    
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
            # adding records using the SQLObject object takes too long
            #self.obj_class(**self.cur_props)
            
            # so we're going to go closer to the metal
            props_for_db = { }
            for k,v in self.cur_props.items():
                # if this is a reference to a FK, massage the values to fit
                if isinstance(v, SQLObject):
                    k += 'Id'
                    v = v.id
                # need to convert the attr names to DB column names
                props_for_db[self.db_style.pythonAttrToDBColumn(k)] = v
            
            self.conn.query(self.conn.sqlrepr(Insert(self.obj_class.sqlmeta.table, values=props_for_db)))
            
        except Exception, e:
            # could not insert this, so ignore the row
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not insert the row ' + repr(self.cur_props))
        
        self.cur_props = None
        
        if self.row_count % 1000 == 0:
            print('%-10s Processed %d rows.' % ('[%s]' % self.obj_class.sqlmeta.table,
                                               self.row_count)
                  )

class BadgeContentHandler(BaseContentHandler):
    """
    Parses the string -
    
    <row Id="15" UserId="6" Name="Supporter" Date="2010-05-19T21:57:31.000" />
    """
    def __init__(self, conn, site):
        BaseContentHandler.__init__(self, conn, site, Badge)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'site' : self.site }
            d['sourceId'] = int(attrs['Id'])
            d['userId'] = int(attrs.get('UserId', 0))
            d['name'] = attrs.get('Name', '')
            d['date'] = attrs.get('Date')
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
    def __init__(self, conn, site):
        BaseContentHandler.__init__(self, conn, site, Comment)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'siteId' : self.site.id }
            d['sourceId'] = int(attrs['Id'])
            d['postId'] = int(attrs.get('PostId', 0))
            d['score'] = int(attrs.get('Score', 0))
            d['text'] = attrs.get('Text', '')
            d['creationDate'] = attrs.get('CreationDate')
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
    def __init__(self, conn, site):
        BaseContentHandler.__init__(self, conn, site, User)
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { 'site' : self.site }
            d['sourceId'] = int(attrs['Id'])
            d['reputation'] = int(attrs.get('Reputation', 0))
            d['creationDate'] = attrs.get('CreationDate')
            d['displayName'] = attrs.get('DisplayName', '')
            d['emailHash'] = attrs.get('EmailHash', '')
            d['lastAccessDate'] = attrs.get('LastAccessDate')
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
    TAGS_RE = re.compile(u'<([^>]+)>')
    
    def __init__(self, solr, site):
        self.solr = solr
        self.site = site
        self.unfinished_questions = { }
        self.orphan_answers = { }
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
            d['id'] = int(attrs['Id'])
            
            if attrs['PostTypeId'] == '2':
                # I am an answer.
                d['parentId'] = int(attrs['ParentId'])
            elif attrs['PostTypeId'] == '1':
                # I am a question.
                d['answers'] = [ ]
                d['answerCount'] = int(attrs.get('AnswerCount', 0))
                d['viewCount'] = int(attrs.get('ViewCount', 0))
            elif attrs['PostTypeId'] == '3':
                raise ValueError('Skipping row ID [%s] as it is an orphaned tag wiki page (PostTypeId [3]).' % (attrs.get('Id', -1)))
            elif attrs['PostTypeId'] == '4':
                raise ValueError('Skipping row ID [%s] as it is a tag wiki excerpt (PostTypeId [4]).' % (attrs.get('Id', -1)))
            elif attrs['PostTypeId'] == '5':
                raise ValueError('Skipping row ID [%s] as it is a tag wiki page (PostTypeId [5]).' % (attrs.get('Id', -1)))
            elif attrs['PostTypeId'] == '6':
                raise ValueError('Skipping row ID [%s] as it is a moderator nomination post (PostTypeId [6]).' % (attrs.get('Id', -1)))
            elif attrs['PostTypeId'] == '7':
                raise ValueError('Skipping row ID [%s] as it is a wiki placeholder page (PostTypeId [7]).' % (attrs.get('Id', -1)))
            elif attrs['PostTypeId'] == '8':
                raise ValueError('Skipping row ID [%s] as it is an privilege wiki page (PostTypeId [8]).' % (attrs.get('Id', -1)))
            else:
                raise ValueError('Unknown PostTypeId [%s] for row ID [%s].' % (attrs.get('PostTypeId', -1), attrs.get('Id', -1)))
            
            if 'AcceptedAnswerId' in attrs:
                d['acceptedAnswerId'] = int(attrs.get('AcceptedAnswerId', 0))
            # Solr accepts ISO dates, but must be UTC as indicated by trailing Z
            d['creationDate'] = attrs.get('CreationDate') + 'Z'
            d['score'] = int(attrs.get('Score', 0))
            d['body'] = attrs.get('Body', '')
            d['ownerUserId'] = int(attrs.get('OwnerUserId', 0))
            if 'LastEditorUserId' in attrs:
                d['lastEditorUserId'] = int(attrs.get('LastEditorUserId', 0))
            if 'LastEditDate' in attrs:
                d['lastEditDate'] = attrs.get('LastEditDate') + 'Z'
            d['lastActivityDate'] = attrs.get('LastActivityDate') + 'Z'
            if 'CommunityOwnedDate' in attrs:
                d['communityOwnedDate'] = attrs.get('CommunityOwnedDate') + 'Z'
            if 'ClosedDate' in attrs:
                d['closedDate'] = attrs.get('ClosedDate') + 'Z'
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
            
            # the cur_props is now complete. Stash it away until question is complete.
            if d.has_key('parentId'):
                # this is an answer.
                if not self.unfinished_questions.has_key(d['parentId']):
                    if not self.orphan_answers.has_key(d['parentId']):
                        self.orphan_answers[d['parentId']] = [ ]
                    self.orphan_answers[d['parentId']].append(d)
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
                    # check if any of the orphan answers are for this question
                    if self.orphan_answers.has_key(d['id']):
                        d['answers'].extend(self.orphan_answers[d['id']])
                        # remove orphan answers from the orphan list
                        del self.orphan_answers[d['id']]
            
        except Exception, e:
            # could not insert this, so ignore the row
            print('Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('Could not process the row ' + repr(self.cur_props))
        
        self.cur_props = None
        
        if self.row_count % 1000 == 0:
            print('%-10s Processed %d rows.' % ('[post]', self.row_count))
        
        # only check for finished questions every 10000 rows to speed things up
        if self.row_count % 10000 == 0:
            print('Committing completed questions...')
            self.commit_finished_questions()
    
    def commit_finished_questions(self):
        # check if any questions are now complete (answerCount=len(answers))
        finished_question_ids = [ ]
        questions_to_commit = [ ]
        for id, q in self.unfinished_questions.items():
            if len(q['answers']) >= q['answerCount']:
                if len(q['answers']) > q['answerCount']:
                    print('Question ID [%s] expected to have %d answers, but has %d instead. Ignoring inconsistency.' % (q['id'], q['answerCount'], len(q['answers'])))
             
                try:
                    # question is complete, store it.
                    questions_to_commit.append(self.finalise_question(q))
        
                except Exception, e:
                    # could not serialise and insert this question, so ignore it
                    print('Exception: ' + str(e))
                    import traceback
                    traceback.print_exc()
                    print('Could not process the completed question ' + repr(q))
            
                finally:
                    finished_question_ids.append(id)
        
        # commit at the end so if cancelled, there won't be orphans in solr.
        self.commit_questions(questions_to_commit, commit=False)
        
        # remove any finished questions from the unfinished list
        for id in finished_question_ids:
            self.unfinished_questions.pop(id)
    
    def finalise_question(self, q):
        """
        Massages and serialises the question object so it can be inserted into
        the search index in the form that we want.
        
        Things this does -
        * fetch comments for question and answers and attach them to the objects
        * creates the 'text' field for the search index that contains all the
          text of the question (title, question, answers and all comments).
        * serialises answers to JSON
        * creates dict that maps to the search index document schema
        * remove unwanted attributes from the q object and serialise question to
          JSON
        * add question JSON to document
        * commit document to search index.
        """
        # find and attach any comments for this question and its answers
        # get the set of post ids
        post_ids = set()
        post_ids.add(q['id'])
        for a in q['answers']:
            post_ids.add(a['id'])
        
        # get the comments
        comment_objs = Comment.select(AND(Comment.q.siteId == self.site.id,
                                          IN(Comment.q.postId, list(post_ids))))
        
        # sort the comments out into a dict keyed on the post id
        comments = { }
        for c in comment_objs:
            # convert comment object to a JSON-serialisable object
            comment_json = { }
            for f in Comment.json_fields:
                comment_json[f] = getattr(c, f)
            
            # we already know that this comment comes from the current site, so
            # we only need to filter on post ID
            if not comments.has_key(c.postId):
                comments[c.postId] = [ ]
            comments[c.postId].append(comment_json)
        
        # add comments to the question
        if comments.has_key(q['id']):
            q['comments'].extend(comments[q['id']])
        
        if len(q['comments']) != q['commentCount']:
            print('Post ID [%s] expected to have %d comments, but has %d instead. Ignoring inconsistency.' % (q['id'], q['commentCount'], len(q['comments'])))
        
        # add comments to the answers
        for a in q['answers']:
            if comments.has_key(a['id']):
                a['comments'].extend(comments[a['id']])
            
            if len(a['comments']) != a['commentCount']:
                print('Post ID [%s] expected to have %d comments, but has %d instead. Ignoring inconsistency.' % (a['id'], a['commentCount'], len(a['comments'])))
        
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
        doc['answers-json'] = [ json.dumps(a, default=self.json_default_handler) for a in q['answers'] ]
        
        # map other fields to search index doc
        # this is the ID for Solr to uniquely identify this question across all
        # sites
        doc['documentId'] = self.site.key + '-' + str(q['id'])
        doc['id'] = str(q['id'])
        doc['siteKey'] = self.site.key
        doc['creationDate'] = q['creationDate']
        # the XML field name is score, but score is a reserved lucene keyword
        doc['votes'] = q['score']
        doc['viewCount'] = q['viewCount']
        doc['title'] = q['title']
        doc['answerId'] = [ str(a['id']) for a in q['answers'] ]
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
            # check that the accepted answer is in the question's answers section.
            # sometimes they're not, e.g. if the question was merged - the
            # acceptedAnswerId would point to an answer in another question
            # instead. We don't deal with merged questions yet, so this option
            # means questions won't appear to have answers when they don't.
            if q['acceptedAnswerId'] in post_ids:
                question_obj['acceptedAnswerId'] = q['acceptedAnswerId']
            else:
                print 'Question [ID# %i] had an unknown answer. Possibly been merged or migrated. Ignoring inconsistency.' % (q['id'], )
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
            question_obj['tags'] = PostContentHandler.TAGS_RE.findall(q['tags'])
        question_obj['favoriteCount'] = q['favoriteCount']
        question_obj['comments'] = q['comments']
        
        doc['question-json'] = json.dumps(question_obj, default=self.json_default_handler)
        
        return doc
    
    def commit_questions(self, questions, commit=True):
        """
        Adds the given list of questions to solr.

        By default, they are committed immediately. Set the ``commit`` argument
        to False to disable this behaviour.

        This function will loop if a SolrError is encountered to allow the user
        to retry the commit without having to start again from the beginning,
        e.g. if the Solr instance stops responding.
        """
        while True:
            try:
                self.solr.add(questions, commit=commit)
                break
            except SolrError, e:
                print('A Solr error occurred while committing questions - ')
                traceback.print_exc(file=sys.stdout)
                print('')
                while True:
                    response = raw_input('Try committing the questions again? (y/n) ').lower()
                    if response not in ('y', 'n'):
                        print("Answer either 'y' or 'n'. Answering 'n' will abort the import process.")
                    else:
                        print('')
                        if response == 'y':
                            break
                        else:
                            raise
            except:
                raise
    
    def commit_all_questions(self):
        """
        Commits all questions, regardless of whether they're completed or not.
        
        Should be called after all XML has been parsed.
        """
        self.commit_finished_questions()
        
        questions_to_commit = [ ]
        for id, q in self.unfinished_questions.items():
            print('Question [ID# %d] was expected to have %d answers, but has %d instead. Ignoring inconsistency.' % (q['id'], q['answerCount'], len(q['answers'])))

            try:
                # question is complete, store it.
                questions_to_commit.append(self.finalise_question(q))
    
            except Exception, e:
                # could not serialise and insert this question, so ignore it
                print('Exception: ' + str(e))
                import traceback
                traceback.print_exc()
                print('Could not process the question ' + repr(q))

        # commit at the end so if cancelled, there won't be orphans in solr.
        self.commit_questions(questions_to_commit, commit=False)

        # we're committing all questions, so nothing is now unfinished
        self.unfinished_questions.clear()
        
        # check if there are any orphan answers
        for question_id, answers in self.orphan_answers.items():
            print('There are %d answers for missing question [ID# %d]. Ignoring orphan answers.' % (len(answers), question_id))


# TEMP COMMENT DATABASE DEFINITION
comment_db_sqlhub = dbconnection.ConnectionHub()
class Comment(SQLObject):
    sourceId = IntCol()
    siteId = IntCol()
    postId = IntCol()
    score = IntCol()
    text = UnicodeCol()
    creationDate = DateTimeCol(datetimeFormat=ISO_DATE_FORMAT)
    userId = IntCol()

    siteId_postId_index = DatabaseIndex(siteId, postId)

    _connection = comment_db_sqlhub

    json_fields = [ 'id', 'score', 'text', 'creationDate', 'userId' ]


# METHODS
def get_file_path(dir_path, filename):
    """
    Returns the absolute path to the file, matching the file in a
    case-insensitive manner.

    If multiple files match the filename (e.g. on case-sensitive filesystems) a
    ValueError will be thrown.

    If no matching files are found, None will be returned.

    @param dir_path: the directory containing the file
    @param filename: the filename to make a case-insensitive match with
    @return: the absolute path to the matched file
    """
    files = [ f for f in os.listdir(dir_path) ]
    matches = [ ]

    for f in files:
        if not os.path.isfile(os.path.join(dir_path, f)):
            continue
        if f.lower() == filename.lower():
            matches.append(f)

    if len(matches) == 0:
        return None

    if len(matches) > 1:
        raise ValueError('More than one file matched the given filename - ' + repr(matches))

    return os.path.abspath(os.path.join(dir_path, matches[0]))


def import_site(xml_root, site_name, dump_date, site_desc, site_key,
                site_base_url, answer_yes=False):
    print('Using the XML root path: ' + xml_root + '\n')

    if not os.path.exists(xml_root):
        print('The given XML root path does not exist.')
        sys.exit(1)

    # connect to the database
    print('Connecting to the Stackdump database...')
    conn_str = settings.DATABASE_CONN_STR
    sqlhub.processConnection = connectionForURI(conn_str)
    print('Connected.\n')

    # connect to solr
    print('Connecting to solr...')
    solr = Solr(settings.SOLR_URL, assume_clean=True)
    # pysolr doesn't try to connect until a request is made, so we'll make a ping request
    try:
        solr._send_request('GET', 'admin/ping')
    except socket.error, e:
        print('Failed to connect to solr - error was: %s' % str(e))
        print('Aborting.')
        sys.exit(2)
    print('Connected.\n')

    # ensure required tables exist
    print("Creating tables if they don't exist...")
    Site.createTable(ifNotExists=True)
    Badge.createTable(ifNotExists=True)
    User.createTable(ifNotExists=True)
    print('Created.\n')

    # SITE INFO
    # only look if they were not specified at the command line; also only if
    # readme.txt exists (they don't in dumps after Aug 2012)
    readme_path = get_file_path(xml_root, 'readme.txt')
    if not (site_name and dump_date) and readme_path:
        # get the site name from the first line of readme.txt. This could be fragile.
        with open(readme_path, 'r') as f:
            site_readme_desc = f.readline().strip()

        # assume if there's a colon in the name, the name part is before, and the date
        # part is after.
        if ':' in site_readme_desc:
            readme_site_name, readme_dump_date = site_readme_desc.split(':')
            readme_site_name = readme_site_name.strip()
            readme_dump_date = readme_dump_date.strip()
        else:
            readme_site_name = site_readme_desc
            readme_dump_date = None

        # if the phrase ' - Data Dump' is in the readme site name, remove it
        i = readme_site_name.rfind(' - Data Dump')
        if i >= 0:
            readme_site_name = readme_site_name[:i].strip()

        if not site_name:
            site_name = readme_site_name
        if not dump_date:
            dump_date = readme_dump_date

    # look for the site in the sites RSS file using the base_url with the id in RSS
    # scrub the URL scheme off the base_url
    if site_base_url:
        # if there is no URL scheme, add one so it can be parsed by urllib2 so it
        # can strip off other bits in the URL that we don't want
        if '://' not in site_base_url:
            site_base_url = 'http://%s' % site_base_url
        site_base_url = urllib2.Request(site_base_url).get_host()

    # attempt to get more information from the sites RSS cache
    if site_base_url and not (site_name and site_desc and site_key):
        sites_file_path = os.path.join(script_dir, '../../../../data/sites')
        if os.path.exists(sites_file_path):
            with open(sites_file_path) as f:
                sites_file = ElementTree.parse(f)
                entries = sites_file.findall('{http://www.w3.org/2005/Atom}entry')

                for entry in entries:
                    entry_base_url = entry.find('{http://www.w3.org/2005/Atom}id').text
                    if '://' in entry_base_url:
                        entry_base_url = urllib2.Request(entry_base_url).get_host()
                    if site_base_url == entry_base_url:
                        # this entry matches the detected site id
                        if not site_key:
                            # extract the key from the url
                            rss_site_key = entry.find('{http://www.w3.org/2005/Atom}id').text
                            # remove the URL scheme
                            if '://' in rss_site_key:
                                rss_site_key = rss_site_key[rss_site_key.find('://')+3:]
                            # remove the TLD
                            if rss_site_key.rfind('.') >= 0:
                                rss_site_key = rss_site_key[:rss_site_key.rfind('.')]
                            # remove the .stackexchange bit
                            if '.stackexchange' in rss_site_key:
                                rss_site_key = rss_site_key[:rss_site_key.find('.stackexchange')]

                            site_key = rss_site_key

                        if not site_name:
                            site_name = entry.find('{http://www.w3.org/2005/Atom}title').text.strip()
                        if not site_desc:
                            site_desc = entry.find('{http://www.w3.org/2005/Atom}summary').text.strip()

    print 'Name: %s\nKey: %s\nDescription: %s\nDump Date: %s\nBase URL: %s\n' % (
        site_name.encode('ascii', 'ignore') if site_name else None, 
        site_key, 
        site_desc.encode('ascii', 'ignore') if site_desc else None, 
        dump_date, 
        site_base_url
    )

    # the base URL is optional.
    if not (site_name and site_key and site_desc and dump_date):
        print 'Could not get all the details for the site.'
        print 'Use command-line parameters to specify the missing details (listed as None).'
        sys.exit(1)

    # prevent importing sites with keys that clash with method names in the app,
    # e.g. a site key of 'search' would clash with the Stackdump-wide search page.
    if site_key in ('search', 'import', 'media', 'licenses'):
        print 'The site key given, %s, is a reserved word in Stackdump.' % site_key
        print 'Use the --site-key parameter to specify an alternate site key.'
        sys.exit(2)

    # confirm site details with user to make sure we don't accidentally overwrite
    # another site.
    if not answer_yes:
        confirm_prompt = 'Are these details correct (answer "yes" to proceed, anything else to abort)? '
        confirm_answer = raw_input(confirm_prompt)
        if confirm_answer != 'yes':
            print 'Import aborted on user request.'
            sys.exit(3)

    # rollback any uncommitted entries in solr. Uncommitted entries may occur if
    # this import process is aborted. Solr doesn't have the concept of transactions
    # like databases do, so without a rollback, we'll be committing the previously
    # uncommitted entries plus the newly imported ones.
    #
    # This also means multiple dataproc processes cannot occur concurrently. If you
    # do the import will be silently incomplete.
    print('Clearing any uncommitted entries in solr...')
    solr._update('<rollback />', waitFlush=None, waitSearcher=None)
    print('Cleared.\n')

    # check if site is already in database; if so, purge the data.
    site = list(Site.select(Site.q.key==site_key))
    if len(site) > 0:
        site = site[0]
        print('Deleting site "%s" from the database... ' % site.name)
        sys.stdout.flush()
        # Using SQLObject to delete rows takes too long, so we're going to do it directly
        #Site.delete(site.id) # the relationship cascades, so other rows will be deleted
        sqlhub.threadConnection = sqlhub.processConnection.transaction()
        conn = sqlhub.threadConnection
        # these deletions are done in this order to avoid FK constraint issues
        print('\tDeleting badges...')
        conn.query(conn.sqlrepr(Delete(Badge.sqlmeta.table, where=(Badge.q.site==site))))
        print('\tDeleting users...')
        conn.query(conn.sqlrepr(Delete(User.sqlmeta.table, where=(User.q.site==site))))
        print('\tDeleting site...')
        conn.query(conn.sqlrepr(Delete(Site.sqlmeta.table, where=(Site.q.id==site.id))))
        sqlhub.threadConnection.commit(close=True)
        print('Deleted.\n')

        print('Deleting site "%s" from the solr... ' % site.name)
        solr.delete(q='siteKey:"%s"' % site.key, commit=False)
        solr.commit(expungeDeletes=True)
        print('Deleted.\n')

    # create the temporary comments database
    print('Connecting to the temporary comments database...')
    temp_db_file, temp_db_path = tempfile.mkstemp('.sqlite', 'temp_comment_db-' + re.sub(r'[^\w]', '_', site_key) + '-', settings.TEMP_COMMENTS_DATABASE_DIR)
    os.close(temp_db_file)
    conn_str = 'sqlite:///' + temp_db_path
    comment_db_sqlhub.processConnection = connectionForURI(conn_str)
    print('Connected.')
    Comment.createTable()
    print('Schema created.')
    comment_db_sqlhub.processConnection.getConnection().execute('PRAGMA synchronous = OFF')
    comment_db_sqlhub.processConnection.getConnection().execute('PRAGMA journal_mode = MEMORY')
    print('Pragma configured.\n')

    timing_start = time.time()

    # start a new transaction
    sqlhub.threadConnection = sqlhub.processConnection.transaction()
    conn = sqlhub.threadConnection
    comment_db_sqlhub.threadConnection = comment_db_sqlhub.processConnection.transaction()
    temp_db_conn = comment_db_sqlhub.threadConnection

    # create a new Site
    site = Site(name=site_name, desc=site_desc, key=site_key, dump_date=dump_date,
                import_date=datetime.now(), base_url=site_base_url)

    # BADGES
    # Processing of badges has been disabled because they don't offer any useful
    # information in the offline situation.
    #print('[badge] PARSING BADGES...')
    #xml_path = get_file_path(xml_root, 'badges.xml')
    #print('[badge] start parsing badges.xml...')
    #handler = BadgeContentHandler(conn, site)
    #xml.sax.parse(xml_path, handler)
    #print('[badge]\tProcessed %d rows.' % (handler.row_count))
    #print('[badge] FINISHED PARSING BADGES.\n')

    # COMMENTS
    # comments are temporarily stored in the database for retrieval when parsing
    # posts only.
    print('[comment] PARSING COMMENTS...')
    xml_path = get_file_path(xml_root, 'comments.xml')
    print('[comment] start parsing comments.xml...')
    handler = CommentContentHandler(temp_db_conn, site)
    xml.sax.parse(xml_path, handler)
    print('%-10s Processed %d rows.' % ('[comment]', handler.row_count))
    print('[comment] FINISHED PARSING COMMENTS.\n')

    # USERS
    print('[user] PARSING USERS...')
    xml_path = get_file_path(xml_root, 'users.xml')
    print('[user] start parsing users.xml...')
    handler = UserContentHandler(conn, site)
    xml.sax.parse(xml_path, handler)
    print('%-10s Processed %d rows.' % ('[user]', handler.row_count))
    print('[user] FINISHED PARSING USERS.\n')

    # POSTS
    # posts are added directly to the Solr index; they are not added to the database.
    print('[post] PARSING POSTS...')
    xml_path = get_file_path(xml_root, 'posts.xml')
    print('[post] start parsing posts.xml...')
    handler = PostContentHandler(solr, site)
    xml.sax.parse(xml_path, handler)
    handler.commit_all_questions()
    print('%-10s Processed %d rows.' % ('[post]', handler.row_count))

    print('[post] FINISHED PARSING POSTS.\n')

    # DELETE COMMENTS
    print('[comment] DELETING TEMPORARY COMMENTS DATABASE (they are no longer needed)...')
    temp_db_conn.commit(close=True)
    comment_db_sqlhub.processConnection.close()
    os.remove(temp_db_path)
    print('[comment] FINISHED DELETING COMMENTS.\n')

    # commit transaction
    print('COMMITTING IMPORTED DATA TO DISK...')
    sqlhub.threadConnection.commit(close=True)
    solr.commit()
    print('FINISHED COMMITTING IMPORTED DATA TO DISK.\n')

    timing_end = time.time()

    print('Time taken for site insertion into Stackdump: %f seconds.' % (timing_end - timing_start))
    print('')

# MAIN METHOD
if __name__ == '__main__':
    parser = OptionParser(usage='usage: %prog [options] xml_root_dir')
    parser.add_option('-n', '--site-name', help='Name of the site.')
    parser.add_option('-d', '--site-desc', help='Description of the site (if not in sites).')
    parser.add_option('-k', '--site-key', help='Key of the site (if not in sites).')
    parser.add_option('-c', '--dump-date', help='Dump date of the site.')
    parser.add_option('-u', '--base-url', help='Base URL of the site on the web.')
    parser.add_option('-Y', help='Answer yes to any confirmation questions.', dest='answer_yes', action='store_true', default=False)

    (cmd_options, cmd_args) = parser.parse_args()

    if len(cmd_args) < 1:
        print('The path to the directory containing the extracted XML files is required.')
        sys.exit(1)

    import_site(cmd_args[0], cmd_options.site_name, cmd_options.dump_date,
                cmd_options.site_desc, cmd_options.site_key,
                cmd_options.base_url, answer_yes=cmd_options.answer_yes)
