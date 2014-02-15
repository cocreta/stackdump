#!/usr/bin/env python

##
# This script provides the ability to manage sites in Stackdump.
##

import os
import sys
import socket
from optparse import OptionParser

from sqlobject import sqlhub, connectionForURI
from pysolr import Solr

from stackdump.models import Site
from stackdump import settings

script_dir = os.path.dirname(sys.argv[0])

# FUNCTIONS
def list_sites():
    # connect to the data sources
    # connect to the database
    print('Connecting to the database...')
    conn_str = settings.DATABASE_CONN_STR
    sqlhub.processConnection = connectionForURI(conn_str)
    print('Connected.\n')
    
    sites = list(Site.select()) # force the lazy method to execute
    
    if len(sites) > 0:
        print('[site key] site name')
        print('-' * 80)
        for site in sites:
            print('[%s] %s' % (site.key, site.name))
    
def delete_site(site_key):
    # connect to the data sources
    # connect to the database
    print('Connecting to the database...')
    conn_str = settings.DATABASE_CONN_STR
    sqlhub.processConnection = connectionForURI(conn_str)
    print('Connected.\n')
    
    # connect to solr
    print('Connecting to solr...')
    solr = Solr(settings.SOLR_URL)
    # pysolr doesn't try to connect until a request is made, so we'll make a ping request
    try:
        solr._send_request('GET', '%s/admin/ping' % solr.path)
    except socket.error, e:
        print('Failed to connect to solr - error was: %s' % str(e))
        print('Aborting.')
        sys.exit(2)
    print('Connected.\n') 
    
    site_name = None
    site = Site.select(Site.q.key==site_key).getOne(None)
    if not site:
        print 'Site key "%s" does not exist in database.\n' % site_key
        # continuing at this point means any orphaned entries in solr are
        # deleted as well.
    else:
        site_name = site.name
        sqlhub.threadConnection = sqlhub.processConnection.transaction()
        
        print('Deleting site "%s" from the database... ' % site.name)
        sys.stdout.flush()
        Site.delete(site.id) # the relationship cascades, so other rows will be deleted
        print('Deleted.\n')
        
        sqlhub.threadConnection.commit(close=True)
    
    # rollback any uncommitted entries in solr. Uncommitted entries may occur if
    # the import process is aborted. Solr doesn't have the concept of 
    # transactions like databases do, so without a rollback, we'll be committing
    # the previously uncommitted entries plus the newly imported ones.
    #
    # This also means multiple dataproc processes cannot occur concurrently. If 
    # you do the import will be silently incomplete.
    print('Clearing any uncommitted entries in solr...')
    solr._update('<rollback />', waitFlush=None, waitSearcher=None)
    print('Cleared.\n')
    
    if site_name:
        print('Deleting site "%s" from solr... ' % site_name)
    else:
        print('Deleting site with key "%s" from solr... ' % site_key)
    solr.delete(q='siteKey:"%s"' % site_key, commit=False)
    solr.commit(expungeDeletes=True)
    print('Deleted.\n')

# END FUNCTIONS

# MAIN METHOD
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-l', '--list-sites', help='List sites imported into Stackdump.', action="store_true")
    parser.add_option('-d', '--delete-site', help='Delete a site from Stackdump.', metavar='SITE_KEY')
    
    (cmd_options, cmd_args) = parser.parse_args()
    
    if cmd_options.list_sites:
        list_sites()
    elif cmd_options.delete_site:
        # confirm with the user first
        answer = raw_input('Are you sure you want to delete %s (type "yes" to delete)? ' % cmd_options.delete_site)
        if answer.lower() != 'yes':
            print 'Aborting on user request.'
            sys.exit(1)
        
        print ''
        delete_site(cmd_options.delete_site)
    else:
        parser.print_help()
