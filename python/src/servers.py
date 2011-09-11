from bottle import ServerAdapter
import sys

class CherryPyServer(ServerAdapter):
    '''
    This copy of bottle's CherryPyServer is necessary so we can set the nodelay
    option to false when running under Jython. Otherwise it will error out as
    the TCP_NODELAY option is not supported with Jython.
    '''
    
    def run(self, handler): # pragma: no cover
        from cherrypy import wsgiserver
        server = wsgiserver.CherryPyWSGIServer((self.host, self.port), handler)
        
        # Jython doesn't work with the TCP_NODELAY option
        if sys.platform.startswith('java'):
            server.nodelay = False
        
        try:
            server.start()
        finally:
            server.stop()

# in order for these to be specified in settings.py, they need to be in the
# following dictionary.
#
# if the name clashes with the default bottle ones, they definition here will
# be used instead.
definitions = {
    'cherrypy' : CherryPyServer
}
