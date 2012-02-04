import sys
import os
import threading
import functools
import re
import math

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

from bottle import route, run, static_file, debug, abort, request, redirect
from jinja2 import Environment, PackageLoader
from sqlobject import sqlhub, connectionForURI, AND, OR, IN, SQLObjectNotFound 
from pysolr import Solr
import iso8601

from stackdump.models import Site, Badge, Comment, User

# STATIC VARIABLES
BOTTLE_ROOT = os.path.abspath(os.path.dirname(sys.argv[0]))
MEDIA_ROOT = os.path.abspath(BOTTLE_ROOT + '/../../media')


# THREAD LOCAL VARIABLES
thread_locals = threading.local()


# CUSTOM TEMPLATE TAGS AND FILTERS

def format_datetime(value):
    '''\
    Formats a datetime to something nice. If a string is given, an attempt will
    be made at parsing it.
    '''
    if isinstance(value, basestring):
        try:
            value = iso8601.parse_date(value)
        except iso8601.ParseError, e:
            # couldn't parse it, so just return what we were given
            return value
    
    return value.strftime('%d %b %Y %I:%M %p')

def set_get_parameters(base_url, *new_parameters):
    '''\
    Adds the provided GET parameters to the base URL, adding a ? if necessary.
    If the GET parameter already exists, it is overridden.
    '''
    parameters = [ ]
    if '?' in base_url:
        parameters = base_url[base_url.find('?')+1:].split('&')
        base_url = base_url[:base_url.find('?')]
    
    for p in new_parameters:
        parameter_name = '=' in p and p[:p.find('=')+1] or p
        
        # check to see if this parameter is already set
        i = 0
        p_replaced = False
        while i < len(parameters):
            cur_p = parameters[i]
            # if it is, just replace it in place
            if cur_p.startswith(parameter_name):
                parameters[i] = p
                p_replaced = True
                break
            
            i += 1
        
        if not p_replaced:
            parameters.append(p)
    
    return '%s?%s' % (base_url, '&'.join(parameters))

# END CUSTOM TEMPLATE TAGS AND FILTERS


# RESOURCE DECORATORS

def uses_templates(fn):
    '''\
    If called without a function, the template environment is initialised and
    returned.
    
    Otherwise, the function is wrapped to ensure the template environment is
    created before the function is executed.\
    '''
    def init_templates():
        if not hasattr(thread_locals, 'template_env'):
            thread_locals.template_env = Environment(
                loader=PackageLoader('stackdump', 'templates'),
                # always auto-escape.
                autoescape=lambda template_name: True,
                # but allow auto-escaping to be disabled explicitly within the
                # template.
                extensions=['jinja2.ext.autoescape']
            )
            thread_locals.template_env.filters['format_datetime'] = format_datetime
            thread_locals.template_env.filters['set_get_parameters'] = set_get_parameters
    
    if not fn:
        init_templates()
        return None
    
    else:
        def wrapped(*args, **kwargs):
            init_templates()
            return fn(*args, **kwargs)
    
        return functools.wraps(fn)(wrapped)

def uses_solr(fn):
    '''\
    If called without a function, the Solr connection is initialised and
    returned.
    
    Otherwise, the function is wrapped to ensure the Solr connection is
    created before the function is executed.\
    '''
    def init_solr():
        if not hasattr(thread_locals, 'solr_conn'):
            thread_locals.solr_conn = Solr("http://localhost:8983/solr/")
    
    if not fn:
        init_solr()
        return None
    
    else:
        def wrapped(*args, **kwargs):
            init_solr()
            return fn(*args, **kwargs)
    
        return functools.wraps(fn)(wrapped)

def uses_db(fn):
    '''\
    If called without a function, the database connection is initialised and
    returned.
    
    Otherwise, the function is wrapped to ensure the database connection is
    created before the function is executed.\
    '''
    def init_db():
        if not hasattr(thread_locals, 'db_conn'):
            db_path = os.path.abspath(os.path.join(BOTTLE_ROOT, '../../../data/stackdump.sqlite'))
            conn_str = 'sqlite://' + db_path
            thread_locals.db_conn = sqlhub.threadConnection = connectionForURI(conn_str)
    
    if not fn:
        init_db()
        return None
    
    else:
        def wrapped(*args, **kwargs):
            init_db()
            return fn(*args, **kwargs)

        return functools.wraps(fn)(wrapped)

# END RESOURCE DECORATORS


# WEB REQUEST METHODS

# all decorators must appear AFTER the route decorators. Any decorators that
# appear above the route decorators will be silently ignored, presumably because
# Bottle caches view functions when the route decorator is called.

# this method MUST sit above the generic static media server, otherwise it won't
# be hit and you will get 'file not found' errors when looking for a
# non-existent logo.
@route('/media/logos/:site_key#[\w\.]+#.png')
def site_logos(site_key):
    root = os.path.join(MEDIA_ROOT, 'images/logos')
    filename = '%s.png' % site_key
    path = os.path.join(root, filename)
    if os.path.exists(path):
        return static_file(filename, root=root)
    else:
        return static_file('images/unknown_site_logo.png', root=MEDIA_ROOT)

# Bottle will protect us against nefarious peeps using ../ hacks.
@route('/media/:filename#.*#')
def serve_static(filename):
    return static_file(filename, root=MEDIA_ROOT)

@route('/')
@uses_templates
@uses_db
def index():
    context = { }
    context['site_root_path'] = ''
    context['sites'] = Site.select()
    return render_template('index.html', context)

@route('/:site_key#[\w\.]+#')
@route('/:site_key#[\w\.]+#/')
@uses_templates
@uses_db
def site_index(site_key):
    context = { }
    context['site_root_path'] = '%s/' % site_key
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        abort(code=404, output='No site exists with the key %s.' % site_key)
    
    return render_template('site_index.html', context)

@route('/search')
@uses_templates
@uses_solr
@uses_db
def search():
    # TODO: scrub this first to avoid Solr injection attacks?
    query = request.GET.get('q')
    if not query:
        redirect(settings.APP_URL_ROOT)
    
    page = int(request.GET.get('p', 0))
    # page needs to be zero-based for pysolr
    page = (page > 0) and (page - 1) or page
    
    rows_per_page = int(request.GET.get('r', 10))
    rows_per_page = (rows_per_page > 0) and rows_per_page or 10
    
    sort_args = {
        'newest' : 'creationDate desc',
        'votes' : 'votes desc',
        'relevance' : 'score desc' # score is the special keyword for the
                                   # relevancy score in Lucene
    }
    sort_by = request.GET.get('s', 'relevance').lower()
    # default to sorting by relevance
    if sort_by not in sort_args.keys():
        sort_by = 'relevance'
    
    # perform search
    results = solr_conn().search(query,
                                 start=page*rows_per_page,
                                 rows=rows_per_page,
                                 sort=sort_args[sort_by])
    decode_json_fields(results)
    retrieve_users(results, question_only=True, ignore_comments=True)
    
    context = { }
    context['site_root_path'] = ''
    context['sites'] = Site.select()
    
    # TODO: scrub this first to avoid HTML injection attacks?
    context['query'] = query
    context['results'] = results
    context['total_hits'] = results.hits
    context['current_page'] = page + 1 # page should be ones-based
    context['rows_per_page'] = rows_per_page
    context['total_pages'] =  int(math.ceil(float(results.hits) / rows_per_page))
    context['sort_by'] = sort_by
    
    return render_template('results.html', context)

@route('/:site_key#[\w\.]+#/search')
@uses_templates
@uses_solr
def site_search(site_key):
    context = { }
    context['site_root_path'] = '%s/' % site_key
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    # TODO: scrub this first to avoid Solr injection attacks?
    query = request.GET.get('q')
    if not query:
        redirect(settings.APP_URL_ROOT)
    
    page = request.GET.get('p', 0)
    rows_per_page = request.GET.get('r', 10)
    
    # perform search
    results = solr_conn().search(query, start=page*rows_per_page, rows=rows_per_page)
    decode_json_fields(results)
    retrieve_users(results)
    
    # TODO: scrub this first to avoid HTML injection attacks?
    context['query'] = query
    context['results'] = results
    
    return render_template('site_results.html', context)

# END WEB REQUEST METHODS


# VIEW HELPERS

def template_env():
    # check that the template environment was initialised
    uses_templates(None)
    return thread_locals.template_env

def solr_conn():
    # check that the Solr connection was initialised
    uses_solr(None)
    return thread_locals.solr_conn

# This method is a bit useless, because the objects aren't accessed directly
# from the connection.
def db_conn():
    # check that the database connection was initialised
    uses_db(None)
    return thread_locals.db_conn

def render_template(template_path, context=None):
    if not context:
        context = { }
    
    context['SETTINGS'] = get_template_settings()
    context['REQUEST'] = request
    
    return template_env().get_template(template_path).render(**context)

def get_template_settings():
    template_settings = { }
    keys = settings.get('TEMPLATE_SETTINGS', [ ])
    for k in keys:
        template_settings[k] = settings.get(k, None)
    
    return template_settings

def decode_json_fields(obj):
    '''\
    Looks for keys in obj that end in -json, decodes the corresponding value and
    stores that in the key minus -json suffix.
    
    If the obj is only a dict, then wrap it in a list because the we also want
    to process list of dicts. If it is not a dict, it is assumed to be a list.\
    '''
    if obj == None:
        return obj
    
    if isinstance(obj, dict):
        objs = [ obj ]
    else:
        objs = obj
    
    for o in objs:
        for k in o.keys():
            if k.endswith('-json'):
                decoded_key = k[:-len('-json')]
                
                json_value = o[k]
                if isinstance(json_value, list):
                    decoded_list = [ ]
                    for j in json_value:
                        decoded_list.append(json.loads(j))
                    
                    o[decoded_key] = decoded_list
                else: # assume it is a JSON string
                    o[decoded_key] = json.loads(json_value)
                
                # remove the JSON string from the dict-object
                del o[k]

def retrieve_users(results, question_only=False, ignore_comments=False):
    '''\
    Retrieves the user objects associated with the question objects.
    '''
    # get a list of all the user IDs
    user_ids_by_site = { }
    for r in results:
        site_name = r['siteName']
        if site_name not in user_ids_by_site.keys():
            user_ids_by_site[site_name] = set()
        
        # the search result object itself
        for k in r.keys():
            if k.lower().endswith('userid'):
                user_ids_by_site[site_name].add(r[k])
        
        # the question object
        question = r['question']
        for k in question.keys():
            if k.lower().endswith('userid'):
                user_ids_by_site[site_name].add(question[k])
            
            comments = question.get('comments')
            if not ignore_comments and comments:
                for c in comments:
                    for ck in c.keys():
                        if ck.lower().endswith('userid'):
                            user_ids_by_site[site_name].add(c[ck])
        
        # the answers
        answers = r.get('answers')
        if not question_only and answers:
            for a in answers:
                for k in a.keys():
                    if k.lower().endswith('userid'):
                        user_ids_by_site[site_name].add(a[k])
                
                comments = a.get('comments')
                if not ignore_comments and comments:
                    for c in comments:
                        for ck in c.keys():
                            if ck.lower().endswith('userid'):
                                user_ids_by_site[site_name].add(c[ck])
    
    # retrieve the user objects from the database by site
    users_by_site = { }
    for site_name in user_ids_by_site.keys():
        site = Site.select(Site.q.name == site_name).getOne()
        user_objects = User.select(AND(User.q.site == site,
                                       IN(User.q.sourceId, list(user_ids_by_site[site_name]))
                                  ))
        
        # convert results into a dict with user id as the key
        users = { }
        for u in user_objects:
            users[u.sourceId] = u
        
        users_by_site[site_name] = users
    
    # place user objects into the dict
    for r in results:
        site_name = r['siteName']
        
        # the search result object itself
        for k in r.keys():
            if k.lower().endswith('userid'):
                # use the same field name, minus the 'Id' on the end.
                r[k[:-2]] = users_by_site[site_name].get(r[k])
        
        # the question object
        question = r['question']
        for k in question.keys():
            if k.lower().endswith('userid'):
                # use the same field name, minus the 'Id' on the end.
                question[k[:-2]] = users_by_site[site_name].get(question[k])
            
        comments = question.get('comments')
        if not ignore_comments and comments:
            for c in comments:
                for ck in c.keys():
                    if ck.lower().endswith('userid'):
                        # use the same field name, minus the 'Id' on the end.
                        c[ck[:-2]] = users_by_site[site_name].get(c[ck])
            
            
        
        # the answers
        answers = r.get('answers')
        if not question_only and answers:
            for a in answers:
                for k in a.keys():
                    if k.lower().endswith('userid'):
                        # use the same field name, minus the 'Id' on the end.
                        a[k[:-2]] = users_by_site[site_name].get(a[k])
                
                comments = a.get('comments')
                if not ignore_comments and comments:
                    for c in comments:
                        for ck in c.keys():
                            if ck.lower().endswith('userid'):
                                # use the same field name, minus the 'Id' on the end.
                                c[ck[:-2]] = users_by_site[site_name].get(c[ck])

# END VIEW HELPERS

# INITIALISATION

if __name__ == '__main__':
    # only do these things in the child processes, not the parents. Applies when
    # the auto-reload option is on (reloader=True). When it is on, the
    # BOTTLE_CHILD env var is True if this is the child process.
    if os.environ.get('BOTTLE_CHILD', True):
        print('Serving media from: %s' % MEDIA_ROOT)
    
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
