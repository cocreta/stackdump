#!/usr/bin/env python

# This script reads a directory containing the Stack Exchange data dump and
# returns a list of sites.

import sys
import os
import re

CONTENT_FILENAME_RE = re.compile(r'^(.+)\.7z$|^(.+)\.7z\.\d{3}$')

if len(sys.argv) != 2:
    print('One argument is expected - the path to the data dump directory.')
    sys.exit(1)

dump_path = sys.argv[1]
print('Using the data dump path: ' + dump_path + '\n')

if not os.path.exists(dump_path):
    print('The given data dump path does not exist.')
    sys.exit(1)

# we expect it to contain an 'Content' directory
dump_path = os.path.join(dump_path, 'Content')
if not os.path.exists(dump_path):
    print('The given data dump path is invalid. The Content subdirectory was expected.')
    sys.exit(1)

filenames = os.listdir(dump_path)
sites = set()

for f in filenames:
    match = CONTENT_FILENAME_RE.match(f)
    if match:
        sites.add(match.group(match.lastindex))

print sites

