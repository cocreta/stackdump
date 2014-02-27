# This is the default settings file for stackdump.
#
# DO NOT CHANGE THIS FILE.
#
# Change the settings.py file; those settings will override the defaults in this
# file.
#
# This file is just like any other Python file, except the local variables form
# the settings dictionary.

DEBUG = False

# see http://bottlepy.org/docs/dev/tutorial.html#multi-threaded-server
SERVER_ADAPTER = 'cherrypy'
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 8080

SOLR_URL = 'http://localhost:8983/solr/stackdump/'

import os
DATABASE_CONN_STR = 'sqlite:///' + os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'stackdump.sqlite')
TEMP_COMMENTS_DATABASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data')

# if the website is hosted under a subpath, specify it here. It must end with a
# slash.
APP_URL_ROOT = '/'

# number of comments to show before the rest are hidden behind a 'click to show'
# link
NUM_OF_DEFAULT_COMMENTS = 3

# number of random questions to show on search query pages
NUM_OF_RANDOM_QUESTIONS = 3

# rewrite links and images to point internally or to a placeholder respectively
REWRITE_LINKS_AND_IMAGES = True

# settings that are available in templates
TEMPLATE_SETTINGS = [
    'APP_URL_ROOT',
    'NUM_OF_DEFAULT_COMMENTS',
    'NUM_OF_RANDOM_QUESTIONS'
]
