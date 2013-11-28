#!/usr/bin/env python

# This file contains all the model definitions for the database.

from sqlobject import SQLObject, UnicodeCol, DateTimeCol, IntCol, ForeignKey, \
                      DatabaseIndex

class Site(SQLObject):
    name = UnicodeCol()
    desc = UnicodeCol()
    key = UnicodeCol()
    dump_date = UnicodeCol()
    import_date = DateTimeCol()
    base_url = UnicodeCol()
    
    siteKey_index = DatabaseIndex(key, unique=True)


class Badge(SQLObject):
    sourceId = IntCol()
    site = ForeignKey('Site', cascade=True)
    userId = IntCol()
    name = UnicodeCol()
    date = DateTimeCol()


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
    
    siteId_sourceId_index = DatabaseIndex(site, sourceId, unique=True)
