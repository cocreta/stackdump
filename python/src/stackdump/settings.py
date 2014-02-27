# This is the settings file for stackdump.
#
# Uncomment lines from this file to override any of the default settings.
#
# This file is just like any other Python file, except the local variables form
# the settings dictionary.

# DO NOT remove this line - this line loads the default settings. Stackdump will
# not work without the default settings.
from default_settings import *

#DEBUG = False

# see http://bottlepy.org/docs/dev/tutorial.html#multi-threaded-server
#SERVER_ADAPTER = 'cherrypy'
#SERVER_HOST = '0.0.0.0'
#SERVER_PORT = 8080

# uncomment if the default host and port for Solr is different.
#SOLR_URL = 'http://localhost:8983/solr/stackdump/'

# uncomment if the database for Stackdump is not the default SQLite one or you
# wish to have the database at a different path to the stackdump_root/data
# directory
#DATABASE_CONN_STR = 'sqlite:///' + path_to_the_database

# if the website is hosted under a subpath, specify it here. It must end with a
# slash.
#APP_URL_ROOT = '/'

# number of comments to show before the rest are hidden behind a 'click to show'
# link
#NUM_OF_DEFAULT_COMMENTS = 3

# number of random questions to show on search query pages
#NUM_OF_RANDOM_QUESTIONS = 3

# rewrite links and images to point internally or to a placeholder respectively
#REWRITE_LINKS_AND_IMAGES = True
