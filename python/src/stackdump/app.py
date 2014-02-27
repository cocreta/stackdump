import sys
import os
import threading
import functools
import re
import math
import random
import urllib2
import socket
from xml.etree import ElementTree

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

from bottle import get, run, static_file, debug, request, error, HTTPError, redirect
from jinja2 import Environment, PackageLoader
from sqlobject import sqlhub, connectionForURI, AND, OR, IN, SQLObjectNotFound
from sqlobject.dberrors import OperationalError
from pysolr import Solr, SolrError
import iso8601
import html5lib
from html5lib.filters._base import Filter as HTML5LibFilterBase
import markdown

from stackdump.models import Site, Badge, User
from stackdump import settings

# STATIC VARIABLES
BOTTLE_ROOT = os.path.abspath(os.path.dirname(sys.argv[0]))
MEDIA_ROOT = os.path.abspath(BOTTLE_ROOT + '/../../media')
SE_QUESTION_ID_RE = re.compile(r'/(questions|q)/(?P<id>\d+)/')
SE_ANSWER_ID_RE = re.compile(r'/a/(?P<id>\d+)/')

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

def get_surrounding_page_numbers(total_pages, cur_page, count=9):
    """
    Returns a list of ints representing pages that surround the cur_page.
    
    For example, if total_pages = 34, cur_page = 11, count = 9, then
    [ 7, 8, 9, 10, 11, 12, 13, 14, 15 ] would be returned.
    """
    # if we can show all the page links, show them all
    if total_pages <= count:
        return range(1, total_pages + 1)
    
    # the -1 is for the current page in the middle
    pages_per_side = int(math.floor((count - 1) / 2))
    left_max = cur_page - pages_per_side
    right_max = cur_page + pages_per_side
    
    if left_max < 1:
        diff = 1 - left_max
        left_max = 1
        right_max += diff
    elif right_max > total_pages:
        diff = right_max - total_pages
        left_max -= diff
        right_max = total_pages
    
    return range(left_max, right_max + 1)

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
            thread_locals.template_env.filters['get_surrounding_page_numbers'] = get_surrounding_page_numbers
    
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
            thread_locals.solr_conn = Solr(settings.SOLR_URL)
    
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
            conn_str = settings.DATABASE_CONN_STR
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

@error(404)
@uses_templates
def error404(error):
    context = { }
    context['error'] = error
    
    return render_template('404.html', context)

@error(500)
@uses_templates
def error500(error):
    ex = error.exception
    if isinstance(ex, NoSitesImportedError):
        return render_template('nodata.html')
    if isinstance(ex, OperationalError):
        # if the error is the database is locked, then it is likely that an
        # import operation is in progress (for SQLite anyway). Return a nice
        # error page for that.
        # HACK: the exception object doesn't seem to provide a better way though.
        if 'database is locked' in ex.args:
            return render_template('importinprogress.html')
        # check if we get a 'no such table' error. If so, this means we haven't
        # had any data imported yet.
        if ex.message.startswith('no such table:'):
            return render_template('nodata.html')
    if isinstance(ex, socket.error):
        # if the error is connection refused, then it is likely because Solr is
        # not running. Show a nice error message.
        if ex.errno == 111:
            return render_template('solrnotrunning.html')
    if isinstance(ex, SolrError):
        # if the error is a Solr error, it is likely a syntax issue
        try:
            # the error is a string, so try parsing it
            # format seems to be "[Reason: blah]\n{"error":{"code":400...}}"
            reason, error_json = ex.message.split('\n')
            error_json = json.loads(error_json)
            if error_json['error']['code'] == 400:
                return render_template('badsolrsyntax.html', { 
                    'reason' : reason,
                    'error' : error_json
                })
        except Exception:
            pass
 
    # otherwise, return the standard error message
    if not settings.DEBUG:
        try:
            return render_template('500.html')
        except: # if there are any errors, just render Bottle's default error page.
            pass
    
    return repr(error)

@get('/')
@uses_templates
@uses_solr
@uses_db
def index():
    context = { }
    context['sites'] = get_sites()
    
    context['random_questions'] = get_random_questions(count=settings.NUM_OF_RANDOM_QUESTIONS)
    
    return render_template('index.html', context)

# this method MUST sit above the site_index and other methods below so it
# cannot be subverted by a site with a site key of 'import'.
@get('/import')
@get('/import/')
@uses_templates
def import_data():
    '''\
    Renders the 'how to import data' page.
    '''
    return render_template('import_data.html')

# this method MUST sit above the site_index and other methods below so it
# cannot be subverted by a site with a site key of 'search'.
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

# this method MUST sit above the site_index and other methods below so it
# cannot be subverted by a site with a site key of 'licenses'.
@get('/licenses/mit')
@get('/licenses/mit/')
@uses_templates
def mit_license():
    '''\
    Renders the 'mit license' page.
    '''
    return render_template('mit_license.html')

# this method MUST sit above the site_index and other methods below so it
# cannot be subverted by a site with a site key of 'licenses'.
@get('/licenses/cc-wiki')
@get('/licenses/cc-wiki/')
@uses_templates
def cc_wiki_license():
    '''\
    Renders the 'cc-wiki license' page.
    '''
    return render_template('cc-wiki_license.html')

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
    
    context['random_questions'] = get_random_questions(site_key=site_key, count=settings.NUM_OF_RANDOM_QUESTIONS)    
    
    return render_template('index.html', context)

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
@get('/:site_key#[\w\.]+#/:question_id#\d+#/:answer_id#\d+#')
@uses_templates
@uses_solr
@uses_db
def view_question(site_key, question_id, answer_id=None):
    context = { }
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    # get the question referenced by this question id
    query = 'id:%s siteKey:%s' % (question_id, site_key)
    results = solr_conn().search(query)
    if len(results) == 0:
        raise HTTPError(code=404, output='No question exists with the ID %s for the site, %s.' % (question_id, context['site'].name))
    
    decode_json_fields(results)
    retrieve_users(results)
    retrieve_sites(results)
    
    result = results.docs[0]
    convert_comments_to_html(result)
    if settings.REWRITE_LINKS_AND_IMAGES:
        rewrite_result(result)
    sort_answers(result)
    context['result'] = result

    context['answer_id'] = answer_id
    
    return render_template('question.html', context)

@get('/:site_key#[\w\.]+#/questions/:question_id#\d+#')
def view_question_redirect(site_key, question_id):
    '''
    Redirects users from the long-form, proper URLs to the shorter one used
    by Stackdump.
    '''
    redirect('%s%s/%s' % (settings.APP_URL_ROOT, site_key, question_id))

@get('/:site_key#[\w\.]+#/a/:answer_id#\d+#')
@uses_templates
@uses_solr
@uses_db
def view_answer(site_key, answer_id):
    context = { }
    
    try:
        context['site'] = Site.selectBy(key=site_key).getOne()
    except SQLObjectNotFound:
        raise HTTPError(code=404, output='No site exists with the key %s.' % site_key)
    
    # get the question referenced by this answer id
    query = 'answerId:%s siteKey:%s' % (answer_id, site_key)
    results = solr_conn().search(query)
    if len(results) == 0:
        raise HTTPError(code=404, output='No answer exists with the ID %s for the site, %s.' % (answer_id, context['site'].name))
    
    question_id = results.docs[0]['id']

    redirect('%s%s/%s/%s' % (settings.APP_URL_ROOT, site_key, question_id, answer_id))

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
    keys = settings.TEMPLATE_SETTINGS
    for k in keys:
        template_settings[k] = getattr(settings, k, None)
    
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
    sites = list(Site.select().orderBy('name'))
    if len(sites) == 0:
        raise NoSitesImportedError()
    
    return sites

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
    site_objects = Site.select(IN(Site.q.key, list(site_keys)))
    
    # convert results into a dict with site key as the key
    sites = { }
    for s in site_objects:
        sites[s.key] = s
    
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

def sort_answers(result):
    '''\
    Sorts the answers in the given result such that the accepted answer (if one)
    is sorted first, and all others are sorted by their votes.
    '''
    answers = result.get('answers')
    if not answers:
        return
    
    accepted_answer_id = result['question'].get('acceptedAnswerId', None)
    
    def comparison_function(a, b):
        if a['id'] == accepted_answer_id:
            return 1
        elif b['id'] == accepted_answer_id:
            return -1
        
        return cmp(a.get('score'), b.get('score'))
    
    answers.sort(comparison_function, reverse=True)

def convert_comments_to_html(results):
    '''\
    Converts the comments in the given result(s) from Markdown to HTML.
    
    Either a single result (a dict) or a list of results (a list of dicts)
    is accepted.
    '''
    markdown_config = {
        'output_format' : 'xhtml5',
        'safe_mode' : 'escape'
    }
    
    # is this a single result?
    if isinstance(results, dict):
        results = [ results ]
    
    for r in results:
        question = r.get('question', { })
        for c in question.get('comments', [ ]):
            c['text'] = markdown.markdown(c.get('text'), **markdown_config)
        
        answers = r.get('answers', [ ])
        for a in answers:
            for c in a.get('comments', [ ]):
                c['text'] = markdown.markdown(c.get('text'), **markdown_config)

def _rewrite_html(html, app_url_root, sites_by_urls):
    
    class HTMLDocElementsFilter(HTML5LibFilterBase):
        '''\
        This filter removes all html, head and body tags, leaving only the HTML
        fragments behind. This is what we want; the extra tags are introduced
        as part of the html5lib processing.
        
        This is needed instead of using the omit_optional_tags parameter on the
        serializer because that also omits optional element tags, e.g. the end
        p tag if the p block is enclosed in another element, which is allowed in
        HTML5.
        '''
        def __iter__(self):
            for token in HTML5LibFilterBase.__iter__(self):
                type = token['type']
                if type in ('StartTag', 'EmptyTag', 'EndTag'):
                    name = token['name']
                    if name in ('html', 'head', 'body'):
                        continue
                
                yield token
    
    # wrap the given HTML fragments in an element so it looks like a document.
    html = '<html>%s</html>' % html
    
    parser = html5lib.HTMLParser(tree=html5lib.treebuilders.getTreeBuilder('etree'))
    html = parser.parse(html)
    
    # rewrite img URLs
    for t in html.iter('{http://www.w3.org/1999/xhtml}img'):
        if t.get('src', None):
            t.set('title', 'Original URL: %s' % t.get('src'))
            t.set('src', '%smedia/images/img_placeholder.png' % app_url_root)
    
    # rewrite link URLs
    for t in html.iter('{http://www.w3.org/1999/xhtml}a'):
        internal_link = False
        url = t.get('href', None)
        if url:
            host = urllib2.Request(url).get_host()
            site = sites_by_urls.get(host, None)
            if site:
                # rewrite this URL for stackdump
                question_id = SE_QUESTION_ID_RE.search(url)
                if question_id:
                    question_id = question_id.groupdict()['id']
                    url = '%s%s/%s' % (app_url_root, site.key, question_id)
                    t.set('href', url)
                    t.set('class', t.get('class', '') + ' internal-link')
                    internal_link = True
                
                answer_id = SE_ANSWER_ID_RE.search(url)
                if answer_id:
                    answer_id = answer_id.groupdict()['id']
                    url = '%s%s/a/%s' % (app_url_root, site.key, answer_id)
                    t.set('href', url)
                    t.set('class', t.get('class', '') + ' internal-link')
                    internal_link = True
            
            if not internal_link:
                t.set('class', t.get('class', '') + ' external-link')
    
    # get a string back
    # this is used instead of ElementTree.tostring because that returns HTML
    # with namespaces to conform to XML.
    walker = html5lib.treewalkers.getTreeWalker('etree', implementation=ElementTree)
    stream = HTMLDocElementsFilter(walker(html))
    serializer = html5lib.serializer.htmlserializer.HTMLSerializer(omit_optional_tags=False,
                                                                   quote_attr_values=True,
                                                                   minimize_boolean_attributes=False,
                                                                   use_trailing_solidus=True,
                                                                   space_before_trailing_solidus=True)
    output_generator = serializer.serialize(stream)
    
    return ''.join(output_generator)

def rewrite_result(result):
    '''\
    Rewrites the HTML in this result (question, answers and comments) so
    links to other StackExchange sites that exist in Stackdump are rewritten,
    links elsewhere are decorated with a CSS class, and all images are replaced
    with a placeholder.
    
    The JSON must have been decoded first.
    '''
    app_url_root = settings.APP_URL_ROOT
    
    # get a list of all the site base URLs
    sites = list(Site.select())
    sites_by_urls = dict([ (s.base_url, s) for s in sites ])
    
    # rewrite question
    question = result.get('question')
    if question:
        question['body'] = _rewrite_html(question.get('body'), app_url_root, sites_by_urls)
        for c in question.get('comments', [ ]):
            c['text'] = _rewrite_html(c.get('text'), app_url_root, sites_by_urls)
    
    # rewrite answers
    answers = result.get('answers')
    if answers:
        for a in answers:
            a['body'] = _rewrite_html(a.get('body'), app_url_root, sites_by_urls)
            for c in a.get('comments', [ ]):
                c['text'] = _rewrite_html(c.get('text'), app_url_root, sites_by_urls)
    


# END VIEW HELPERS

# INITIALISATION

if __name__ == '__main__':
    # only do these things in the child processes, not the parents. Applies when
    # the auto-reload option is on (reloader=True). When it is on, the
    # BOTTLE_CHILD env var is True if this is the child process.
    if os.environ.get('BOTTLE_CHILD', 'false') == 'true':
        print('Serving media from: %s' % MEDIA_ROOT)
    
    debug(settings.DEBUG)
    
    # run the server!
    server = settings.SERVER_ADAPTER
    
    run(
        server=server,
        host=settings.SERVER_HOST,
        port=settings.SERVER_PORT,
        reloader=True
    )

# END INITIALISATION
