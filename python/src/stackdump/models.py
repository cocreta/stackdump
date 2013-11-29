#!/usr/bin/env python

# This file contains all the model definitions for the database.

from sqlobject import SQLObject, UnicodeCol, DateTimeCol, IntCol, ForeignKey, \
                      DatabaseIndex


ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


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
    date = DateTimeCol(datetimeFormat=ISO_DATE_FORMAT)


class User(SQLObject):
    sourceId = IntCol()
    site = ForeignKey('Site', cascade=True)
    reputation = IntCol()
    creationDate = DateTimeCol(datetimeFormat=ISO_DATE_FORMAT)
    displayName = UnicodeCol()
    emailHash = UnicodeCol()
    lastAccessDate = DateTimeCol(datetimeFormat=ISO_DATE_FORMAT)
    websiteUrl = UnicodeCol()
    location = UnicodeCol()
    age = IntCol()
    aboutMe = UnicodeCol()
    views = IntCol()
    upVotes = IntCol()
    downVotes = IntCol()
    
    siteId_sourceId_index = DatabaseIndex(site, sourceId, unique=True)
