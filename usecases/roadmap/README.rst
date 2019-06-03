=======================================================
Epiviz File Server with NIH Roadmap Epigenomics Project
=======================================================

In this usecase, we will setup and run the epiviz file server using data from the NIH Roadmap
Epigenomics Project. BioConductor's AnnotationHub provides a list of all the files and their 
metadata for this repository. The API for AnnotationHub is available at Annotation Hub API is hosted at https://annotationhub.bioconductor.org/.

We first download the AnnotationHub sqlite database to query for available resources

.. code-block:: console

    > wget http://annotationhub.bioconductor.org/metadata/annotationhub.sqlite3

After downloading the resource database, we can now load the 
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

We now filter these resources for files from the Roadmap project. 

.. code-block:: python

    roadmap = pd.query('dataprovider=="BroadInstitute" and genome=="hg19"')
    roadmap

After filtering for resources, we can load them into the file server using the 
`import_ahub` helper function.

.. code-block:: python

    mMgr = MeasurementManager()
    ahub_measurements = mMgr.import_ahub(roadmap)
    ahub_measurements