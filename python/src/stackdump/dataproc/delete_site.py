#!/usr/bin/env python

# This script deletes the site specified by the ID in the first parameter.

import os
import sys

from sqlobject import sqlhub, connectionForURI, AND, OR
from pysolr import Solr

from stackdump.models import Site

script_dir = os.path.dirname(sys.argv[0])

if len(sys.argv) < 2:
    print 'The site ID needs to be specified as the first parameter.'
    sys.exit(1)

# connect to the data sources
db_path = os.path.abspath(os.path.join(script_dir, '../../../../data/stackdump.sqlite'))

# connect to the database
print('Connecting to the database...')
conn_str = 'sqlite://' + db_path
sqlhub.processConnection = connectionForURI(conn_str)
print('Connected.\n')

# connect to solr
print('Connecting to solr...')
solr = Solr("http://localhost:8983/solr/")
print('Connected.\n')

site_id = int(sys.argv[1])
site = Site.select(Site.q.id==site_id).getOne(None)
if not site:
    print 'Site ID %d does not exist.' % site_id
    sys.exit(1)

site_name = site.name
print('Deleting site "%s" from the database... ' % site.name)
sys.stdout.flush()
Site.delete(site.id) # the relationship cascades, so other rows will be deleted
print('Deleted.\n')

print('Deleting site "%s" from solr... ' % site_name)
solr.delete(q='siteName:"%s"' % site_name)
print('Deleted.\n')
