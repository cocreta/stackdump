#!/usr/bin/env python

# This file contains all the model definitions for the database.

from sqlobject import *

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