========
Tutorial
========


`This blog post <https://epiviz.github.io/post/2019-02-04-epiviz-fileserver/>`_ 
(Jupyter notebook) describes various features of the file server library 
using genomic files hosted from the
`NIH Roadmap Epigenomics project <http://www.roadmapepigenomics.org/>`_.

.. note::

    This post describes a general walkthrough of the features of the file server. 
    More usecases will be posted soon!

Import Measurements from File
=============================

Since large data repositories contains hundreds of files, manually adding files would be cumbersome. 
In order to make this process easier, we create a configuration file that lists all files with their locations.
An example configuration file is described below - 

Configuration file
==================

The following is a configuration file for data hosted on the roadmap FTP server. This contains data
for ChIP-seq experiments for the H3k27me3 marker in `Esophagus` and `Sigmoid Colon` tissues. 
Most fields in the configuration file are self explanatory. 

.. code-block:: json

    [
        {
            url: "https://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E079-H3K27me3.fc.signal.bigwig",
            file_type: "bigwig",
            datatype: "bp",
            name: "E079-H3K27me3",
            id: "E079-H3K27me3",
            annotation: {
                group: "digestive",
                tissue: "Esophagus",
                marker: "H3K27me3"
            }
        }, {
            url: "https://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E106-H3K27me3.fc.signal.bigwig",
            file_type: "bigwig",
            datatype: "bp",
            name: "E106-H3K27me3",
            id: "E106-H3K27me3",
            annotation: {
                group: "digestive",
                tissue: "Sigmoid Colon",
                marker: "H3K27me3"
            }
        }
    ]

Once the configuration file is generated, we can import these measurements into the file server. We first create 
a `MeasurementManager` object which handles measurements from files and databases. 
We can then use the helper function `import_files` to import all measurements from this configuration file.

.. code-block:: python

    mMgr = MeasurementManager()
    fmeasurements = mMgr.import_files(os.getcwd() + "/roadmap.json", mHandler)
    fmeasurements

Query for a genomic location
============================

After loading the measurements, we can query the object for data in a particular genomic region using the
`get_data` function.

.. code-block:: python

    result, err = await fmeasurements[1].get_data("chr11", 10550488, 11554489)
    result.head()

The reponse is a tuple, DataFrame that contains all results and an error if there is any. 


Compute a Function over files
=============================

We can define and create new measurements that can be computed using a `Numpy` function over 
the files loaded from the previous step. 

.. note::

    you can also write a custom statistical function, that applies to every row in the DataFrame. 
    It must follow the same syntax as any `Numpy` row-apply function.

As an example to demonstrate, we can calculate the average ChIP-seq expression for `H3K27me3` marker.


.. code-block:: python

    computed_measurement = mMgr.add_computed_measurement("computed", "avg_ChIP_seq", "Average ChIP seq expression", 
                                            measurements=fmeasurements, computeFunc=numpy.mean)


After defining a computed measurement, we can query this measurement for a genomic location.

.. code-block:: python

    result, err = await computed_measurement.get_data("chr11", 10550488, 11554489)
    result.head()

Setup a REST API
================

Often times, developers would like to include data from
genomic files into a web application for visualization or 
into their workflows. We can quickly setup a REST API web 
server from the measurements we loaded -

.. code-block:: python

    from epivizfileserver import setup_app
        app = setup_app(mMgr)
        app.run(port=8000)

The REST API is an asynchronous web server that is built on top of `SANIC <https://sanic.readthedocs.io/en/latest/>`_.

Query Files from AnnotationHub
==============================

We can also use the Bioconductor's AnnotationHub to search for files 
and setup the file server. We are working on simplifying this process.

Annotation Hub API is hosted at https://annotationhub.bioconductor.org/. 

We first download the annotationhub sqlite database for available data resources.

.. code-block:: console

    wget http://annotationhub.bioconductor.org/metadata/annotationhub.sqlite3

After download the resource database from AnnotatiobnHub, we can now load the 
sqlite database into python and query for datasets.

.. code-block:: python

    import pandas
    import os
    import sqlite3

    conn = sqlite3.connect("annotationhub.sqlite3")
    cur = conn.cursor()
    cur.execute("select * from resources r JOIN input_sources inp_src ON r.id = inp_src.resource_id;")
    results = cur.fetchall()
    pd = pandas.DataFrame(results, columns = ["id", "ah_id", "title", "dataprovider", "species", "taxonomyid", "genome", 
                                            "description", "coordinate_1_based", "maintainer", "status_id",
                                            "location_prefix_id", "recipe_id", "rdatadateadded", "rdatadateremoved",
                                            "record_id", "preparerclass", "id", "sourcesize", "sourceurl", "sourceversion",
                                            "sourcemd5", "sourcelastmodifieddate", "resource_id", "source_type"])
    pd.head()

For the purpose of the tutorial, we will filter for Sigmoid Colon ("E106") and Esophagus ("E079") tissues, 
and the ChipSeq Data for "H3K27me3" histone marker files from the roadmap epigenomics project.

.. code-block:: python

    roadmap = pd.query('dataprovider=="BroadInstitute" and genome=="hg19"')
    roadmap = roadmap.query('title.str.contains("H3K27me3") and (title.str.contains("E106") or title.str.contains("E079"))')
    # only use fc files
    roadmap = roadmap.query('title.str.contains("fc")')
    roadmap

After filtering for resources we are interested in, we can load them into the file server using the 
`import_ahub` helper function.

.. code-block:: python

    mMgr = MeasurementManager()
    ahub_measurements = mMgr.import_ahub(roadmap)
    ahub_measurements

The rest of the process is similar as described in the beginning of this tutorial.
