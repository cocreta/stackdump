import sys
import os
import threading
import functools
import re
import math
import random

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

from bottle import get, run, static_file, debug, request, error, HTTPError
from jinja2 import Environment, PackageLoader
from sqlobject import sqlhub, connectionForURI, AND, OR, IN, SQLObjectNotFound
from sqlobject.dberrors import OperationalError
from pysolr import Solr
import iso8601

from stackdump.models import Site, Badge, Comment, User

# STATIC VARIABLES
BOTTLE_ROOT = os.path.abspath(os.path.dirname(sys.argv[0]))
MEDIA_ROOT = os.path.abspath(BOTTLE_ROOT + '/../../media')


# THREAD LOCAL VARIABLES
thread_locals = threading.local()


# CUSTOM TEMPLATE TAGS AND FILTERS

def format_datetime(value, format_string=None):
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
    
    format_string = format_string or '%d %b %Y %I:%M %p'
    
    return value.strftime(format_string)

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
@get('/media/logos/:site_key#[\w\.]+#.png')
def site_logos(site_key):
    root = os.path.join(MEDIA_ROOT, 'images/logos')
    filename = '%s.png' % site_key
    path = os.path.join(root, filename)
    if os.path.exists(path):
        return static_file(filename, root=root)
    else:
        return static_file('images/unknown_site_logo.png', root=MEDIA_ROOT)

# Bottle will protect us against nefarious peeps using ../ hacks.
@get('/media/:filename#.*#')
def serve_static(filename):
    return static_file(filename, root=MEDIA_ROOT)

@error(500)
@uses_templates
def error500(error):
    ex = error.exception
    if isinstance(ex, NoSitesImportedError):
        return render_template('nodata.html')
    
    # otherwise, return the standard error message
    return repr(error)

@get('/')
@uses_templates
@uses_solr
@uses_db
def index():
    context = { }
    context['sites'] = get_sites()
    
    context['random_questions'] = get_random_questions()
    
    return render_template('index.html', context)

@get('/:site_key#[\w\.]+#')
@get('/:site_key#[\w\.]+#/')
@uses_templates
@uses_solr
@uses_db
def site_index(site_key):
    context = { }
    context['sites'] = get_sites()
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    context['random_questions'] = get_random_questions(site_key=site_key)    
    
    return render_template('index.html', context)

@get('/search')
@uses_templates
@uses_solr
@uses_db
def search():
    context = { }
    context['sites'] = get_sites()
    
    search_context = perform_search()
    if not search_context:
        raise HTTPError(code=500, output='Invalid query attempted.')
    
    context.update(search_context)
    
    return render_template('results.html', context)

@get('/:site_key#[\w\.]+#/search')
@uses_templates
@uses_solr
@uses_db
def site_search(site_key):
    context = { }
    # the template uses this to allow searching on other sites
    context['sites'] = get_sites()
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    # perform the search limited by this site
    search_context = perform_search(site_key)
    if not search_context:
        raise HTTPError(code=500, output='Invalid query attempted.')
    
    context.update(search_context)
    
    return render_template('site_results.html', context)

@get('/:site_key#[\w\.]+#/:question_id#\d+#')
@uses_templates
@uses_solr
@uses_db
def view_question(site_key, question_id):
    context = { }
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    # get the question referenced by this question id
    query = 'id:%s siteKey:%s' % (question_id, site_key)
    results = solr_conn().search(query)
    if len(results) == 0:
        raise HTTPError(code=404, output='No question exists with the id %s.' % question_id)
    
    decode_json_fields(results)
    retrieve_users(results)
    retrieve_sites(results)
    
    context['result'] = results.docs[0]
    
    return render_template('question.html', context)

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

class NoSitesImportedError(Exception):
    def __init__(self, cause=None):
        self.cause = cause
    
    def __str__(self):
        s = 'NoSitesImportedError('
        if self.cause:
            s += str(type(self.cause)) + ' ' + str(self.cause)
        s += ')'
        
        return s

def get_sites():
    '''\
    Retrieves a list of Site objects or if there are none, raises a
    NoSitesImportedError. This error is designed to trigger the 500 error
    handler.
    '''
    try:
        sites = list(Site.select())
        if len(sites) == 0:
            raise NoSitesImportedError()
        
        return sites
    except OperationalError as e:
        raise NoSitesImportedError(e)

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
        site_key = r['siteKey']
        if site_key not in user_ids_by_site.keys():
            user_ids_by_site[site_key] = set()
        
        # the search result object itself
        for k in r.keys():
            if k.lower().endswith('userid'):
                user_ids_by_site[site_key].add(r[k])
        
        # the question object
        question = r['question']
        for k in question.keys():
            if k.lower().endswith('userid'):
                user_ids_by_site[site_key].add(question[k])
            
            comments = question.get('comments')
            if not ignore_comments and comments:
                for c in comments:
                    for ck in c.keys():
                        if ck.lower().endswith('userid'):
                            user_ids_by_site[site_key].add(c[ck])
        
        # the answers
        answers = r.get('answers')
        if not question_only and answers:
            for a in answers:
                for k in a.keys():
                    if k.lower().endswith('userid'):
                        user_ids_by_site[site_key].add(a[k])
                
                comments = a.get('comments')
                if not ignore_comments and comments:
                    for c in comments:
                        for ck in c.keys():
                            if ck.lower().endswith('userid'):
                                user_ids_by_site[site_key].add(c[ck])
    
    # retrieve the user objects from the database by site
    users_by_site = { }
    for site_key in user_ids_by_site.keys():
        site = Site.select(Site.q.key == site_key).getOne()
        user_objects = User.select(AND(User.q.site == site,
                                       IN(User.q.sourceId, list(user_ids_by_site[site_key]))
                                  ))
        
        # convert results into a dict with user id as the key
        users = { }
        for u in user_objects:
            users[u.sourceId] = u
        
        users_by_site[site_key] = users
    
    # place user objects into the dict
    for r in results:
        site_key = r['siteKey']
        
        # the search result object itself
        for k in r.keys():
            if k.lower().endswith('userid'):
                # use the same field name, minus the 'Id' on the end.
                r[k[:-2]] = users_by_site[site_key].get(r[k])
        
        # the question object
        question = r['question']
        for k in question.keys():
            if k.lower().endswith('userid'):
                # use the same field name, minus the 'Id' on the end.
                question[k[:-2]] = users_by_site[site_key].get(question[k])
            
        comments = question.get('comments')
        if not ignore_comments and comments:
            for c in comments:
                for ck in c.keys():
                    if ck.lower().endswith('userid'):
                        # use the same field name, minus the 'Id' on the end.
                        c[ck[:-2]] = users_by_site[site_key].get(c[ck])
        
        # the answers
        answers = r.get('answers')
        if not question_only and answers:
            for a in answers:
                for k in a.keys():
                    if k.lower().endswith('userid'):
                        # use the same field name, minus the 'Id' on the end.
                        a[k[:-2]] = users_by_site[site_key].get(a[k])
                
                comments = a.get('comments')
                if not ignore_comments and comments:
                    for c in comments:
                        for ck in c.keys():
                            if ck.lower().endswith('userid'):
                                # use the same field name, minus the 'Id' on the end.
                                c[ck[:-2]] = users_by_site[site_key].get(c[ck])

def retrieve_sites(results):
    '''\
    Retrieves the site objects associated with the results.
    '''
    # get a list of all the site keys
    site_keys = set()
    for r in results:
        site_keys.add(r['siteKey'])
    
    # retrieve the site objects from the database
    sites = { }
    for site_key in site_keys:
        sites[site_key] = Site.select(Site.q.key == site_key).getOne()
    
    # place site objects into the dict
    for r in results:
        site_key = r['siteKey']
        r['site'] = sites[site_key]

def perform_search(site_key=None):
    '''\
    Common code for performing a search and returning the context for template
    rendering.
    
    If a site_key was provided, the search will be limited to that particular
    site.
    '''
    # TODO: scrub this first to avoid Solr injection attacks?
    query = request.GET.get('q')
    if not query:
        return None
    # this query string contains any special bits we add that we don't want
    # the user to see.
    int_query = query
    if site_key:
        int_query += ' siteKey:%s' % site_key
    
    # the page GET parameter is zero-based
    page = int(request.GET.get('p', 0))
    if page < 0: page = 0
    
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
    results = solr_conn().search(int_query,
                                 start=page*rows_per_page,
                                 rows=rows_per_page,
                                 sort=sort_args[sort_by])
    decode_json_fields(results)
    retrieve_users(results, question_only=True, ignore_comments=True)
    retrieve_sites(results)
    
    context = { }
    
    # TODO: scrub this first to avoid HTML injection attacks?
    context['query'] = query
    context['results'] = results
    context['total_hits'] = results.hits
    context['current_page'] = page + 1 # page template var is ones-based
    context['rows_per_page'] = rows_per_page
    context['total_pages'] =  int(math.ceil(float(results.hits) / rows_per_page))
    context['sort_by'] = sort_by
    
    return context

def get_random_questions(site_key=None, count=3):
    random_field_name = 'random_%d %s' % (random.randint(1000, 9999), random.choice(['asc', 'desc']))
    query = '*:*'
    if site_key:
        query = ' siteKey:%s' % site_key
        
    results = solr_conn().search(query, rows=count, sort=random_field_name)
    decode_json_fields(results)
    retrieve_users(results, question_only=True, ignore_comments=True)
    retrieve_sites(results)
    
    return results

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
