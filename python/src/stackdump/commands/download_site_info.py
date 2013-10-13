#!/usr/bin/env python

# This script downloads the sites RSS file and associated logos from the net.

import urllib
from xml.etree import ElementTree
import os
import sys

script_dir = os.path.dirname(sys.argv[0])
sites_file_path = os.path.join(script_dir, '../../../../data/sites')

# ensure the data directory exists
if not os.path.exists(os.path.dirname(sites_file_path)):
    os.mkdir(os.path.dirname(sites_file_path))

# download the sites RSS file
print 'Downloading StackExchange sites RSS file...',
urllib.urlretrieve('http://stackexchange.com/feeds/sites', sites_file_path)
print 'done.'

print ''

# parse sites RSS file and download logos
logos_dir_path = os.path.join(script_dir, '../../../media/images/logos')
if not os.path.exists(logos_dir_path):
    os.mkdir(logos_dir_path)

with open(sites_file_path) as f:
    sites_file = ElementTree.parse(f)
    entries = sites_file.findall('{http://www.w3.org/2005/Atom}entry')
    
    for entry in entries:
        entry_title = entry.find('{http://www.w3.org/2005/Atom}title').text.encode('ascii', 'ignore')
        
        # extract the key from the url - remove the http:// and .com
        site_key = entry.find('{http://www.w3.org/2005/Atom}id').text
        if site_key.startswith('http://'):
            site_key = site_key[len('http://'):]
        if site_key.endswith('.com'):
            site_key = site_key[:-len('.com')]
        if site_key.endswith('.stackexchange'):
            site_key = site_key[:-len('.stackexchange')]
        
        print 'Downloading logo for %s...' % entry_title,
        urllib.urlretrieve('http://sstatic.net/%s/img/icon-48.png' % site_key, os.path.join(logos_dir_path, '%s.png' % site_key))
        print 'done.'
