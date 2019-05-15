==================
Epiviz File Server
==================

.. image:: https://readthedocs.org/projects/epivizfileparser/badge/?version=latest
    :target: https://epivizfileparser.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://travis-ci.org/epiviz/epivizFileParser.svg?branch=master
    :target: https://travis-ci.org/epiviz/epivizFileParser

Compute and Query Parser for Genomic Files


Description
===========


Epiviz file Server is a Python library, to query genomic files, 
not only for visualization but also for transformation. 
The library provides various modules to perform various tasks - 
- Parser to read various genomic file formats, 
- Query to access only necessary bytes of file, 
- Compute to apply transformations on data, 
- Server to instantly convert the datasets into an API and 
- Visualization. 


A quick overview of the library and its features, are described in an IPython notebook 
available at - https://epiviz.github.io/post/2019-02-04-epiviz-fileserver/

Note
====
 
1. The library requires the server hosting the data files to support HTTP range requests so that the file server's parser module can only request the necessary byte-ranges needed to process the query
2. The library currently supports indexed genomic file formats like BigWig, BigBed, Bam (with bai), Sam (with sai) or any genomic data file that can be indexed using tabix.

Developer Notes
===============

This project has been set up using PyScaffold 3.1. For details and usage
information on PyScaffold see https://pyscaffold.org/.

use a virtualenv for testing & development. 
To setup run the following commands from the project directory

.. code-block:: python

    virtualenv env --python=python3
    source env/bin/activate # (activate.fish if using the fish-shell)
    pip install -r requirements.txt

    # to deactivate virtualenv
    deactivate

1. Test - ```python setup.py test```
2. Docs - ```python setup.py docs```
3. Build
    - source distribution  ```python setup.py sdist```
    - binary distribution  ```python setup.py bdist```
    - wheel  distribution  ```python setup.py bdist_wheel```
