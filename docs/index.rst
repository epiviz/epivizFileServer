================================================================================
Epiviz File Server - Query & Transform Data from Indexed Genomic Files in Python
================================================================================

Epiviz file Server is a scalable data query and compute system for 
indexed genomic files. In addition to querying data, users can also 
compute transformations, summarization and aggregation using 
`NumPy <https://www.numpy.org/>`_ functions directly on data queried from files. 


Since the genomic files are indexed, the library will only request and parse 
necessary bytes from these files to process the request (without loading the 
entire file into memory). We implemented a cache system 
to efficiently manage already accessed bytes of a file. 
We also use `dask <https://dask.org/>`_ to parallelize computing requests for 
query and transformation. This allows us to process and scale our system to large 
data repositories.

`This blog post <https://epiviz.github.io/post/2019-02-04-epiviz-fileserver/>`_ 
(Jupyter notebook) describes various features of the file server library 
using genomic files hosted from the
`NIH Roadmap Epigenomics project <http://www.roadmapepigenomics.org/>`_.

The library provides various modules to  
    - Parser: Read various genomic file formats, 
    - Query: Access only necessary bytes of file for a given genomic location, 
    - Compute:  Apply transformations on data, 
    - Server:  Instantly convert the datasets into a REST API
    - Visualization: Interactive Exploration of data using Epiviz (uses the Server module above).

.. note::

    - The Epiviz file Server is an open source project on `GitHub <https://github.com/epiviz/epivizFileParser>`_
    - Let us know what you think and any feedback or feature requests to improve the library!



Contents
========

.. toctree::
   :maxdepth: 2

   Installation <installation>
   Tutorial <tutorial>
   License <license>
   Authors <authors>
   Changelog <changelog>
   Module Reference <api/modules>


Indices and tables
================== 

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _toctree: http://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html
.. _reStructuredText: http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html
.. _references: http://www.sphinx-doc.org/en/stable/markup/inline.html
.. _Python domain syntax: http://sphinx-doc.org/domains.html#the-python-domain
.. _Sphinx: http://www.sphinx-doc.org/
.. _Python: http://docs.python.org/
.. _Numpy: http://docs.scipy.org/doc/numpy
.. _SciPy: http://docs.scipy.org/doc/scipy/reference/
.. _matplotlib: https://matplotlib.org/contents.html#
.. _Pandas: http://pandas.pydata.org/pandas-docs/stable
.. _Scikit-Learn: http://scikit-learn.org/stable
.. _autodoc: http://www.sphinx-doc.org/en/stable/ext/autodoc.html
.. _Google style: https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings
.. _NumPy style: https://numpydoc.readthedocs.io/en/latest/format.html
.. _classical style: http://www.sphinx-doc.org/en/stable/domains.html#info-field-lists
