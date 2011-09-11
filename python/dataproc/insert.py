#!/usr/bin/env python

# This script takes the path of a set of 
# returns a list of sites.

import sys
import os
import xml.sax
from datetime import datetime

from sqlobject import *

# MODELS
class Badge(SQLObject):
    userId = IntCol()
    name = StringCol()
    date = DateTimeCol()

# SAX HANDLERS
ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

class BadgeContentHandler(xml.sax.ContentHandler):
    """
    Parses the string -
    
    <row Id="15" UserId="6" Name="Supporter" Date="2010-05-19T21:57:31.000" />
    """
    def __init__(self):
        self.cur_props = None
    
    def startElement(self, name, attrs):
        if name != 'row':
            return
        
        try:
            d = self.cur_props = { }
            d['userId'] = int(attrs.get('UserId', 0))
            d['name'] = attrs.get('Name', '')
            d['date'] = datetime.strptime(attrs.get('Date'), ISO_DATE_FORMAT)
        except Exception, e:
            # could not parse this, so ignore the row completely
            self.cur_props = None
            print('[badge] Exception: ' + str(e))
            print('[badge] Could not parse the row ' + repr(attrs))
    
    def endElement(self, name):
        if name != 'row':
            return
        
        if not self.cur_props:
            return
        
        # the cur_props is now complete. Save it.
        try:
            # the object is automatically saved to the database on creation
            Badge(**self.cur_props)
        except Exception, e:
            # could not insert this, so ignore the row
            print('[badge] Exception: ' + str(e))
            import traceback
            traceback.print_exc()
            print('[badge] Could not insert the row ' + repr(self.cur_props))
        
        self.cur_props = None


# MAIN METHOD
if len(sys.argv) != 2:
    print('One argument is expected - the path to the extracted XML files.')
    sys.exit(1)

xml_root = sys.argv[1]
print('Using the XML root path: ' + xml_root + '\n')

if not os.path.exists(xml_root):
    print('The given XML root path does not exist.')
    sys.exit(1)

temp_db_path = '/tmp/stackdump_import_temp.sqlite'
if os.path.exists(temp_db_path):
    os.remove(temp_db_path)

# create the temp database
sqlhub.processConnection = connectionForURI('jython_sqlite://' + temp_db_path)

# BADGES
print('[badge] PARSING BADGES...')
print('[badge] creating badge table...')
Badge.createTable()
xml_path = os.path.join(xml_root, 'badges.xml')
print('[badge] start parsing badges.xml...')
xml.sax.parse(xml_path, BadgeContentHandler())
print('[badge] FINISHED PARSING BADGES.\n')

