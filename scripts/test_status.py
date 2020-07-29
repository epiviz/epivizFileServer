from epivizfileserver import MeasurementManager, setup_app, create_fileHandler, FileMeasurement
import os

data_path = os.getcwd() + "/tests/data/"
mMgr = MeasurementManager()
mHandler = create_fileHandler()

file_config = [
    {
        "url": data_path + "test.bw",
        "file_type": "bigwig",
        "datatype": "bp",
        "name": "test-bw",
        "id": "test-bw",
        "annotation": {
            "file_type": "bigwig"
        },
        "metadata": []
    }, {
        "url": data_path + "test.bigBed",
        "file_type": "bigbed",
        "datatype": "bp",
        "name": "test-bb",
        "id": "test-bb",
        "annotation": {
            "file_type": "bigbed"
        },
        "metadata": []
    }, {
      "url": "http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E068-H3K4me3.fc.signal.bigwig",
      "file_type": "bigwig",
      "datatype": "bp",
      "name": "Anterior Caudate H3K4me3",
      "id": "Brain_Anterior_Caudate_H3K4me3",
      "annotation": {
        "source": "roadmap",
        "type": "Brain_Anterior_Caudate",
        "marker": "H3K4me3"
      },
      "metadata": []
    }
]

for rec in file_config:
    isGene = False
    if "annotation" in rec["datatype"]:
        isGene = True
    
    tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                    rec.get("url"), annotation=rec.get("annotation"),
                    metadata=rec.get("metadata"), minValue=0, maxValue=5,
                    isGenes=isGene, fileHandler=mHandler
                )
    mMgr.measurements.append(tempFileM)


app = setup_app(mMgr)

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8000)
