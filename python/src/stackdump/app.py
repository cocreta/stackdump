import sys
import os

from bottle import route, run, static_file, debug, abort, request, redirect
from jinja2 import Environment, PackageLoader
from sqlobject import sqlhub, connectionForURI, AND, OR, SQLObjectNotFound 
from pysolr import Solr

from stackdump.models import Site, Badge, Comment, User

# STATIC VARIABLES
BOTTLE_ROOT = os.path.abspath(os.path.dirname(sys.argv[0]))
MEDIA_ROOT = os.path.abspath(BOTTLE_ROOT + '/../../media')

# hopefully this is thread-safe; not sure though. Will need to experiment/check.
# TODO: thread-safe?
TEMPLATE_ENV = Environment(loader=PackageLoader('stackdump', 'templates'))

# WEB REQUEST METHODS

# Bottle will protect us against nefarious peeps using ../ hacks.
@route('/media/:filename#.*#')
def serve_static(filename):
    return static_file(filename, root=MEDIA_ROOT)

@route('/')
def index():
    context = { }
    context['site_root_path'] = ''
    context['sites'] = Site.select()
    return render_template('index.html', context)

@route('/:site_key#\w+#')
@route('/:site_key#\w+#/')
def site_index(site_key):
    context = { }
    context['site_root_path'] = '%s/' % site_key
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        abort(code=404, output='No site exists with the key %s.' % site_key)
    
    return render_template('site_index.html', context)

@route('/search')
def search():
    query = request.GET.get('q')
    if not query:
        redirect(settings.APP_URL_ROOT)
    
    page = request.GET.get('p', 0)
    rows_per_page = request.GET.get('r', 10)
    
    # perform search
    results = solr.search(query, start=page*rows_per_page, rows=rows_per_page)
    
    context = { }
    # TODO: scrub this first to avoid injection attacks?
    context['query'] = query
    context['results'] = results
    
    return render_template('results.html', context)

@route('/:site_key#\w+#/search')
def site_search(site_key):
    context = { }
    context['site_root_path'] = '%s/' % site_key
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    query = request.GET.get('q')
    if not query:
        redirect(settings.APP_URL_ROOT)
    
    page = request.GET.get('p', 0)
    rows_per_page = request.GET.get('r', 10)
    
    # perform search
    results = solr.search(query, start=page*rows_per_page, rows=rows_per_page)
    
    # TODO: scrub this first to avoid injection attacks?
    context['query'] = query
    context['results'] = results
    
    return render_template('site_results.html', context)

# END WEB REQUEST METHODS

# VIEW HELPERS

def render_template(template_path, context=None):
    if not context:
        context = { }
    
    context['SETTINGS'] = get_template_settings()
    
    return TEMPLATE_ENV.get_template(template_path).render(**context)

def get_template_settings():
    template_settings = { }
    keys = settings.get('TEMPLATE_SETTINGS', [ ])
    for k in keys:
        template_settings[k] = settings.get(k, None)
    
    return template_settings

# END VIEW HELPERS

# INITIALISATION

if __name__ == '__main__':
    # only do these things in the child processes, not the parents. Applies when
    # the auto-reload option is on (reloader=True). When it is on, the
    # BOTTLE_CHILD env var is True if this is the child process.
    if os.environ.get('BOTTLE_CHILD', True):
        print('Serving media from: %s' % MEDIA_ROOT)
        
        # connect to the data sources
        db_path = os.path.abspath(os.path.join(BOTTLE_ROOT, '../../../data/stackdump.sqlite'))
    
        # connect to the database
        # TODO: thread-safe?
        print('Connecting to the database...')
        conn_str = 'sqlite://' + db_path
        sqlhub.processConnection = connectionForURI(conn_str)
        print('Connected.\n')
        
        # connect to solr
        # TODO: thread-safe?
        print('Connecting to solr...')
        solr = Solr("http://localhost:8983/solr/")
        print('Connected.\n')
    
    # load the settings file
    __import__('settings')
    if 'settings' in sys.modules.keys():
        settings = sys.modules.get('settings')
        settings = dict([ (k, getattr(settings, k)) for k in dir(settings) if not k.startswith('__') ])
    else:
        settings = { }
    
    if settings.get('DEBUG', False):
        debug(True)
    
    # run the server!
    server = settings.get('SERVER_ADAPTER', 'wsgiref')
    
    run(
        server=server,
        host=settings.get('SERVER_HOST', '0.0.0.0'),
        port=settings.get('SERVER_PORT', 8080),
        reloader=True
    )

# END INITIALISATION
