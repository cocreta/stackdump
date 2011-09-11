from bottle import route, run
import servers
import sys

# WEB REQUEST METHODS

@route('/hello')
def hello():
    return "Hello World!"

# END WEB REQUEST METHODS

# INITIALISATION

# load the settings file
__import__('settings')
if 'settings' in sys.modules.keys():
    settings = sys.modules.get('settings')
    settings = dict([ (k, getattr(settings, k)) for k in dir(settings) if not k.startswith('__') ])
else:
    settings = { }

# run the server!
server = settings.get('SERVER_ADAPTER', 'wsgiref')
# look for definitions in server, otherwise let bottle decide
server = servers.definitions.get(server, server)

run(
    server=server,
    host=settings.get('SERVER_HOST', '0.0.0.0'),
    port=settings.get('SERVER_PORT', 8080)
)

# END INITIALISATION
