from bottle import route, run, static_file
import servers

import sys
import os

# STATIC VARIABLES
BOTTLE_ROOT = os.path.abspath(os.path.dirname(sys.argv[0]))
MEDIA_ROOT = os.path.abspath(BOTTLE_ROOT + '/../media')

# WEB REQUEST METHODS

# Bottle will protect us against nefarious peeps using ../ hacks.
@route('/media/:filename#.*#')
def serve_static(filename):
    return static_file(filename, root=MEDIA_ROOT)

@route('/hello')
def hello():
    return "Hello World!"

# END WEB REQUEST METHODS

# INITIALISATION

if __name__ == '__main__':
    # only print this on the parent process, not the child ones. Applies when
    # the auto-reload option is on (reloader=True). When it is on, the
    # BOTTLE_CHILD env var is True if this is the child process.
    if not os.environ.get('BOTTLE_CHILD', True):
        print('Serving media from: %s' % MEDIA_ROOT)
    
    # load the settings file
    __import__('settings')
    if 'settings' in sys.modules.keys():
        settings = sys.modules.get('settings')
        settings = dict([ (k, getattr(settings, k)) for k in dir(settings) if not k.startswith('__') ])
    else:
        settings = { }
    
    # run the server!
    server = settings.get('SERVER_ADAPTER', 'wsgiref')
    
    run(
        server=server,
        host=settings.get('SERVER_HOST', '0.0.0.0'),
        port=settings.get('SERVER_PORT', 8080),
        reloader=True
    )

# END INITIALISATION
