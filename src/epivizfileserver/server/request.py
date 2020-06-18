from . import utils
import pandas as pd
import ujson
import sys
import os
from ..handler import FileHandlerProcess
import asyncio
from ..measurements import FileMeasurement

def create_request(action, request):
    """
    Create appropriate request class based on action

    Args:
        action : Type of request
        request : Other request parameters

    Returns:
        An instance of EpivizRequest class
    """

    req_manager = {
        "getSeqInfos": SeqInfoRequest,
        "getMeasurements": MeasurementRequest,
        "getData": DataRequest,
        "getCombined": DataRequest,
        "getRows": DataRequest,
        "getValues": DataRequest,
        "search": SearchRequest, 
        "fileQuery": FileRequest
    }

    return req_manager[action](request)

class EpivizRequest(object):
    """
    Base class to process requests
    """

    def __init__(self, request):
        self.request = request
        self.params = None
        self.query = None

    def validate_params(self, request):
        """
        Validate parameters for requests
        
        Args:
            request: dict of params from request
        """
        raise Exception("NotImplementedException")

    def get_data(self, mMgr):
        """
        Get Data for this request type

        Returns:
            result: JSON response for this request
            error: HTTP ERROR CODE
        """
        raise Exception("NotImplementedException")

class SeqInfoRequest(EpivizRequest):
    """
    SeqInfo requests class
    """

    def __init__(self, request):
        super(SeqInfoRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.seqs = {"hg18":[["chr1",1,247249719],["chr2",1,242951149],["chr3",1,199501827],["chr4",1,191273063],["chr5",1,180857866],["chr6",1,170899992],["chr7",1,158821424],["chr8",1,146274826],["chr9",1,140273252],["chr10",1,135374737],["chr11",1,134452384],["chr12",1,132349534],["chr13",1,114142980],["chr14",1,106368585],["chr15",1,100338915],["chr16",1,88827254],["chr17",1,78774742],["chr18",1,76117153],["chr19",1,63811651],["chrX",1,154913754],["chrY",1,57772954]],"hg19":[["chr1",1,249250621],["chr2",1,243199373],["chr3",1,198022430],["chr4",1,191154276],["chr5",1,180915260],["chr6",1,171115067],["chr7",1,159138663],["chr8",1,146364022],["chr9",1,141213431],["chr10",1,135534747],["chr11",1,135006516],["chr12",1,133851895],["chr13",1,115169878],["chr14",1,107349540],["chr15",1,102531392],["chr16",1,90354753],["chr17",1,81195210],["chr18",1,78077248],["chr19",1,59128983],["chrX",1,155270560],["chrY",1,59373566]],"mm9":[["chr1",1,197195432],["chr2",1,181748087],["chr3",1,159599783],["chr4",1,155630120],["chr5",1,152537259],["chr6",1,149517037],["chr7",1,152524553],["chr8",1,131738871],["chr9",1,124076172],["chr10",1,129993255],["chr11",1,121843856],["chr12",1,121257530],["chr13",1,120284312],["chr14",1,125194864],["chr15",1,103494974],["chr16",1,98319150],["chr17",1,95272651],["chr18",1,90772031],["chr19",1,61342430],["chrX",1,166650296],["chrY",1,15902555]],"mm10":[["chr1",1,195471971],["chr2",1,182113224],["chr3",1,160039680],["chr4",1,156508116],["chr5",1,151834684],["chr6",1,149736546],["chr7",1,145441459],["chr8",1,129401213],["chr9",1,124595110],["chr10",1,130694993],["chr11",1,122082543],["chr12",1,120129022],["chr13",1,120421639],["chr14",1,124902244],["chr15",1,104043685],["chr16",1,98207768],["chr17",1,94987271],["chr18",1,90702639],["chr19",1,61431566],["chrX",1,171031299],["chrY",1,91744698]]}

    def validate_params(self, request):
        return None

    async def get_data(self, mMgr):

        genome = self.seqs
        error = None

        return genome, error

class MeasurementRequest(EpivizRequest):
    """
    Measurement requests class
    """
    def __init__(self, request):
        super(MeasurementRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        return None

    async def get_data(self, mMgr):

        result = {
            "annotation": [],
            "datasourceGroup": [],
            "datasourceId": [],
            "defaultChartType": [],
            "id": [],
            "maxValue": [],
            "minValue": [],
            "name": [],
            "type": [],
            "metadata": []
        }        
        error = None

        try:
            measurements = mMgr.get_measurements()

            for rec in measurements:
                result.get("annotation").append(rec.annotation)
                result.get("datasourceGroup").append(rec.mid)
                result.get("datasourceId").append(rec.source)
                result.get("defaultChartType").append("track")
                result.get("id").append(rec.mid)
                result.get("maxValue").append(rec.minValue)
                result.get("minValue").append(rec.maxValue)
                result.get("name").append(rec.name)

                result_type = "feature"
                if rec.isGenes:
                    result_type = "range"
                result.get("type").append(result_type)

                result.get("metadata").append(rec.metadata)

        except Exception as e:
            error = e

        return result, error

class DataRequest(EpivizRequest):
    """
    Data requests class
    """
    def __init__(self, request):
        super(DataRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        params_keys = ["datasource", "seqName", "start", "end", "measurement"]
        params = {"start": 1}

        for key in params_keys:
            if key in request:
                params[key] = request.get(key)
                if key == "start" and params.get(key) in [None, ""]:
                    params[key] = 1
                elif key == "end" and params.get(key) in [None, ""]:
                    params[key] = sys.maxsize
                elif key == "seqName" and params.get(key) in [None, "", "all"]:
                    params[key] = None
                elif key == "metadata[]":
                    del params["metadata[]"]
                    params["metadata"] = request.getlist(key)
                elif key == "measurement":
                    # del params["measurement"]
                    params["measurement"] = params.get("measurement").split(",")
                elif key == "measurements[]":
                    del params["measurements[]"]
                    params["measurement"] = request.getlist(key)
                    if "genes" in params.get("measurement"):
                        params["measurement"] = None
            else:
                if key not in ["measurement", "measurements[]", "start"]:
                # if key not in ["measurement"]:
                    raise Exception("missing params in request", key)
        return params

    async def get_data(self, mMgr):
        measurements = mMgr.get_measurements()
        result = None

        try:
            for rec in measurements:
                if "getRows" in self.request.get("action"):
                    if rec.mid == self.params.get("datasource"):
                        result, err = await rec.get_data(self.params.get("seqName"), 
                                    int(self.params.get("start")), 
                                    int(self.params.get("end")),
                                    self.request.get("bins")
                                )
                        break
                else:
                    if rec.mid in self.params.get("measurement"):
                        # legacy support for browsers that do not send this param
                        if "bins" not in self.request.keys():
                            tbins = 400
                        else:
                            tbins = int(self.request.get("bins"))
                        result, err = await rec.get_data(self.params.get("seqName"), 
                                    int(self.params.get("start")), 
                                    int(self.params.get("end")),
                                    tbins
                                )
                        break
            
            # result = result.to_json(orient='records')
            result = utils.format_result(result, self.params)
            if self.request.get("action") == "getRows":
                return result["rows"], None
            else:
                return result, None
        except Exception as e:
            print("failed in req get_data", str(e))
            return {}, str(e)

class SearchRequest(EpivizRequest):
    """
    Search requests class
    """
    def __init__(self, request):
        super(SearchRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        params_keys = ["q", "maxResults"]
        params = {}

        for key in params_keys:
            if key in request:
                params[key] = request.get(key)
            else:
                raise Exception("missing params in request", key)

        return params

    async def get_data(self, mMgr):
        
        result = None

        try:
            genome = mMgr.genome[mMgr.genome['gene'].str.contains(self.params.get("q"), na=False, case=False)]
            result = []

            if len(genome) > 0:
                # print(len(genome))
                for index, row in genome.iterrows():
                    # print(row)
                    result.append({"gene": row["gene"], "chr": row["chr"], "start": row["start"], "end": row["end"]})

            return result, None
        except Exception as e:
            return {}, str(e)

class FileRequest(EpivizRequest):
    """
    File requests class
    """
    def __init__(self, request):
        super(FileRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        params_keys = ["datasource", "seqName", "start", "end", "measurement"]
        params = {"start": 1}

        for key in params_keys:
            if key in request:
                params[key] = request.get(key)
                if key == "start" and params.get(key) in [None, ""]:
                    params[key] = 1
                elif key == "end" and params.get(key) in [None, ""]:
                    params[key] = sys.maxsize
                elif key == "seqName" and params.get(key) in [None, "", "all"]:
                    params[key] = None
                elif key == "metadata[]":
                    del params["metadata[]"]
                    params["metadata"] = request.getlist(key)
                elif key == "measurement":
                    # del params["measurement"]
                    params["measurement"] = params.get("measurement").split(",")
                elif key == "measurements[]":
                    del params["measurements[]"]
                    params["measurement"] = request.getlist(key)
                    if "genes" in params.get("measurement"):
                        params["measurement"] = None
            else:
                if key not in ["measurement", "measurements[]", "start"]:
                # if key not in ["measurement"]:
                    raise Exception("missing params in request", key)
        return params

    async def get_data(self, mHandler):
        result = None

        try:
            # legacy support for browsers that do not send this param
            if "bins" not in self.request.keys():
                tbins = 400
            else:
                tbins = int(self.request.get("bins"))

            if "getRows" in self.request.get("action"):
                filesep = self.params.get("datasource").split(".")
                # rec = None

                if filesep[len(filesep) - 1] in ["bigbed", "bb"]:
                    rec = FileMeasurement("bigbed", self.params.get("datasource"), self.params.get("datasource"), 
                            self.params.get("datasource"), annotation=None,
                            metadata=None, minValue=0, maxValue=5,
                            isGenes=True, fileHandler=mHandler
                            )
                # elif filesep[len(filesep) - 1] in ["tabix", "tbx", "tbi", "gz"]:
                else:
                    url = "http://obj.umiacs.umd.edu/genomes/"
                    if "hg19" in self.params.get("datasource"):
                        genome = "hg19"
                    elif "mm9" in self.params.get("datasource"):
                        genome = "mm9"
                    elif "mm10" in self.params.get("datasource"):
                        genome = "mm10"
                    else: 
                        genome = None
                        return [], ""
                    
                    gurl = url + genome + "/" + genome + ".txt.gz"
                    
                    rec = FileMeasurement("tabix", genome, genome, 
                                gurl, annotation={"group": "genome"},
                                metadata=["geneid", "exons_start", "exons_end", "gene"], minValue=0, maxValue=5,
                                isGenes=True, fileHandler=mHandler, 
                                columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]
                            )

                result, err = await rec.get_data(self.params.get("seqName"), 
                            int(self.params.get("start")), 
                            int(self.params.get("end")),
                            tbins
                        )
            else:
                print("in else")
                filesep = self.params.get("datasource").split(".")
                print(filesep)

                if filesep[len(filesep) - 1] in ["bigwig", "bw"]:
                    rec = FileMeasurement(filesep[len(filesep) - 1], self.params.get("measurement")[0], self.params.get("datasource"), 
                                self.params.get("datasource"), annotation=None,
                                metadata=None, minValue=0, maxValue=5,
                                isGenes=False, fileHandler=mHandler
                                )

                    result, err = await rec.get_data(self.params.get("seqName"), 
                                int(self.params.get("start")), 
                                int(self.params.get("end")),
                                tbins
                            )
            
            # result = result.to_json(orient='records')
            result = utils.format_result(result, self.params)
            if self.request.get("action") == "getRows":
                return result["rows"], None
            else:
                return result, None
        except Exception as e:
            print("failed in req get_data", str(e))
            return {}, str(e)