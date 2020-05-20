# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config
from datetime import datetime
import os
import sys

import pkg_resources

# -- Path setup --------------------------------------------------------------
sys.path.append(os.path.abspath('../'))

# -- Project information -----------------------------------------------------
try:
    version = pkg_resources.get_distribution('iib').version
except pkg_resources.DistributionNotFound:
    version = 'unknown'
project = 'IIB Image Builder Service'
copyright = datetime.today().strftime('%Y') + ', Red Hat Inc.'
author = 'Red Hat - EXD'

# -- General configuration ---------------------------------------------------
extensions = [
    'celery.contrib.sphinx',
    'recommonmark',
    'sphinx.ext.autodoc',
    'sphinx.ext.githubpages',
]
master_doc = 'index'
language = None
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
pygments_style = 'sphinx'

# -- Options for HTML output -------------------------------------------------
html_theme = 'sphinx_rtd_theme'
html_static_path = []

# -- Options for HTMLHelp output ---------------------------------------------
htmlhelp_basename = 'IIBdoc'

# -- Extension configuration -------------------------------------------------
# This must be mocked because Read the Docs doesn't have krb5-devel installed
autodoc_mock_imports = ["requests_kerberos"]

# -- Options for intersphinx extension ---------------------------------------
# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {'https://docs.python.org/3': None}
