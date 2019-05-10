========
Tutorial
========


`This blog post <https://epiviz.github.io/post/2019-02-04-epiviz-fileserver/>`_ 
(Jupyter notebook) describes various features of the file server library 
using genomic files hosted from the
`NIH Roadmap Epigenomics project <http://www.roadmapepigenomics.org/>`_.


Configuration file
==================

If you have access to a list of publicly available files, one can define a configuration file
to load these into the epiviz file server. The following is a configuration file for data hosted on
the roadmap FTP server.

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

.. note::

    more usecases coming soon!
