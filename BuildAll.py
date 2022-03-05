import shutil, os, py2exe, sys, zipfile
from distutils.core import setup

sys.argv.append('py2exe')

filename = 'BackblazeDownloader.py'
setup(        
    options = {'py2exe': {
        'bundle_files': 3,
        'compressed': True,
        'includes': ['lxml', 'lxml._elementpath']
    }},
    console = [{'script': filename}],
    zipfile = None
    )
