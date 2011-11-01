# This is the settings file for stackdump.
#
# It is modelled after the Django settings file. This file is just like any
# other Python file, except the local variables form the settings dictionary.

DEBUG = True

# see http://bottlepy.org/docs/dev/tutorial.html#multi-threaded-server
SERVER_ADAPTER = 'cherrypy'
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 8080

# if the website is hosted under a subpath, specify it here. It must end with a
# slash.
APP_URL_ROOT = '/'

# settings that are available in templates
TEMPLATE_SETTINGS = [
    'APP_URL_ROOT'
]