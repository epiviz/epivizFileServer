from . import utils
import pandas as pd
import ujson
import sys
import os
from ..handler import FileHandlerProcess
# import asyncio
# import logging
import time
from math import fsum

# logger = logging.getLogger(__name__)
from sanic.log import logger as logging


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
        "search": SearchRequest
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
        self.seqs = {"hg19":[["chr1",1,247249719],["chr2",1,242951149],["chr3",1,199501827],["chr4",1,191273063],["chr5",1,180857866],["chr6",1,170899992],["chr7",1,158821424],["chr8",1,146274826],["chr9",1,140273252],["chr10",1,135374737],["chr11",1,134452384],["chr12",1,132349534],["chr13",1,114142980],["chr14",1,106368585],["chr15",1,100338915],["chr16",1,88827254],["chr17",1,78774742],["chr18",1,76117153],["chr19",1,63811651],["chrX",1,154913754],["chrY",1,57772954]],"hg38":[["chr1",1,249250621],["chr2",1,243199373],["chr3",1,198022430],["chr4",1,191154276],["chr5",1,180915260],["chr6",1,171115067],["chr7",1,159138663],["chr8",1,146364022],["chr9",1,141213431],["chr10",1,135534747],["chr11",1,135006516],["chr12",1,133851895],["chr13",1,115169878],["chr14",1,107349540],["chr15",1,102531392],["chr16",1,90354753],["chr17",1,81195210],["chr18",1,78077248],["chr19",1,59128983],["chrX",1,155270560],["chrY",1,59373566]],"mm9":[["chr1",1,197195432],["chr2",1,181748087],["chr3",1,159599783],["chr4",1,155630120],["chr5",1,152537259],["chr6",1,149517037],["chr7",1,152524553],["chr8",1,131738871],["chr9",1,124076172],["chr10",1,129993255],["chr11",1,121843856],["chr12",1,121257530],["chr13",1,120284312],["chr14",1,125194864],["chr15",1,103494974],["chr16",1,98319150],["chr17",1,95272651],["chr18",1,90772031],["chr19",1,61342430],["chrX",1,166650296],["chrY",1,15902555]],"mm10":[["chr1",1,195471971],["chr2",1,182113224],["chr3",1,160039680],["chr4",1,156508116],["chr5",1,151834684],["chr6",1,149736546],["chr7",1,145441459],["chr8",1,129401213],["chr9",1,124595110],["chr10",1,130694993],["chr11",1,122082543],["chr12",1,120129022],["chr13",1,120421639],["chr14",1,124902244],["chr15",1,104043685],["chr16",1,98207768],["chr17",1,94987271],["chr18",1,90702639],["chr19",1,61431566],["chrX",1,171031299],["chrY",1,91744698]]}

    def validate_params(self, request):
        return None

    async def get_data(self, mMgr):

        try:
            seqs = {}
            for genome, file in mMgr.genomes.items():
                seqs[genome] = file.chromosomes
            
            return seqs, None
        
        except Exception as e:
            return self.seqs, str(e)

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
        genomes = mMgr.get_genomes()
        result = None
        err = None
        
        logging.debug("Request GetData: %s\t%s" % (self.request.get("requestId"), "getRows"))

        try:
            if self.params.get("datasource") in genomes:
                file = genomes[self.params.get("datasource")]
                start = time.time()
                result, err = await file.get_data(self.params.get("seqName"), 
                                    int(self.params.get("start")), 
                                    int(self.params.get("end")))

                end = time.time()
                if self.params.get("datasource") not in  mMgr.stats["getRows"]:
                    mMgr.stats["getRows"][self.params.get("datasource")] = {"sum": 0, "count": 0, "sumSquares": 0}

                mMgr.stats["getRows"][self.params.get("datasource")]["sum"] += (end-start)
                mMgr.stats["getRows"][self.params.get("datasource")]["count"] += 1
                mMgr.stats["getRows"][self.params.get("datasource")]["sumSquares"] += ((end-start) ** 2)
            else:
                for rec in measurements:
                    if "getRows" in self.request.get("action"):
                        if rec.mid == self.params.get("datasource"):
                            logging.debug("Request processing: %s\t%s" % (self.request.get("requestId"), "getRows"))
                            start = time.time()
                            result, err = await rec.get_data(self.params.get("seqName"), 
                                        int(self.params.get("start")), 
                                        int(self.params.get("end")),
                                        self.request.get("bins")
                                    )
                            end = time.time()
                            if self.params.get("datasource") not in  mMgr.stats["getRows"]:
                                mMgr.stats["getRows"][self.params.get("datasource")] = {"sum": 0, "count": 0, "sumSquares": 0}

                            mMgr.stats["getRows"][self.params.get("datasource")]["sum"] += (end-start)
                            mMgr.stats["getRows"][self.params.get("datasource")]["count"] += 1
                            mMgr.stats["getRows"][self.params.get("datasource")]["sumSquares"] += ((end-start) ** 2)
                            break
                    else:
                        if rec.mid in self.params.get("measurement"):
                            # legacy support for browsers that do not send this param
                            if "bins" not in self.request.keys():
                                tbins = 400
                            else:
                                tbins = int(self.request.get("bins"))

                            logging.debug("Request processing: %s\t%s" % (self.request.get("requestId"), "getValues"))
                            start = time.time()
                            result, err = await rec.get_data(self.params.get("seqName"), 
                                        int(self.params.get("start")), 
                                        int(self.params.get("end")),
                                        tbins
                                    )
                            end = time.time()
                            if self.params.get("measurement")[0] not in  mMgr.stats["getValues"]:
                                mMgr.stats["getValues"][self.params.get("measurement")[0]] = {"sum": 0, "count": 0, "sumSquares": 0}

                            mMgr.stats["getValues"][self.params.get("measurement")[0]]["sum"] += (end-start)
                            mMgr.stats["getValues"][self.params.get("measurement")[0]]["count"] += 1
                            mMgr.stats["getValues"][self.params.get("measurement")[0]]["sumSquares"] += ((end-start) ** 2)
                            break
            
            # result = result.to_json(orient='records')
            if result is not None :
                logging.debug("Request processing: %s\t%s" % (self.request.get("requestId"), "format_result"))
                result = utils.format_result(result, self.params)
            if self.request.get("action") == "getRows":
                return result["rows"], err
            else:
                return result, err
        except Exception as e:
            # print("failed in req get_data", str(e))
            logging.error("Data Request: %s" % (self.params), exc_info=True)
            return utils.format_result(pd.DataFrame(columns = ["chr", "start", "end"]), self.params), str(err) + " --- " + str(e)

class SearchRequest(EpivizRequest):
    """
    Search requests class
    """
    def __init__(self, request):
        super(SearchRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        params_keys = ["q", "maxResults", "genome"]
        params = {}

        for key in params_keys:
            if key in request:
                params[key] = request.get(key)
            else:
                raise Exception("missing params in request", key)

        return params

    async def get_data(self, mMgr):
        measurements = mMgr.get_measurements()
        genomes = mMgr.get_genomes()
        result = []
        err = None

        try:
            start = time.time()
            if self.params.get("genome") in genomes and len(self.params.get("q")) > 1:
                file = genomes[self.params.get("genome")]
                result, err = await file.searchGene(self.params.get("q"), self.params.get("maxResults"))
            end = time.time()
            if self.params.get("genome") not in  mMgr.stats["search"]:
                mMgr.stats["search"][self.params.get("genome")] = {"sum": 0, "count": 0, "sumSquares": 0}

            mMgr.stats["search"][self.params.get("genome")]["sum"] += (end-start)
            mMgr.stats["search"][self.params.get("genome")]["count"] += 1
            mMgr.stats["search"][self.params.get("genome")]["sumSquares"] += ((end-start) ** 2)
            return result, err
        except Exception as e:
            logging.error("Search Request: %s" % (self.params.get("genome")), exc_info=True)
            return {}, str(e)

class StatusRequest(EpivizRequest):
    def __init__(self, request, datasource):
        super(StatusRequest, self).__init__(request)
        self.datasource = datasource

    async def get_status(self, mMgr):
        measurements = mMgr.get_measurements()
        genomes = mMgr.get_genomes()
        result = 0
        err = None

        try:
            if self.datasource in genomes:
                file = genomes[self.datasource]
                result, err = await file.get_status()
            else:
                for rec in measurements:
                    if rec.mid == self.datasource:
                        result, err = await rec.get_status()
                        break
            return result, err
        except Exception as e:
            logging.error("Status Request: %s" % (self.datasource), exc_info=True)
            # print("failed in req get_data", str(e))
            return 0, str(err) + " --- " + str(e)

class UpdateCollectionsRequest(EpivizRequest):
    def __init__(self, request):
        super(UpdateCollectionsRequest, self).__init__(request)

    async def update_collections(self, mMgr, fileHandler):
        result = []
        err = None

        # TODO: this should be async
        result, err = mMgr.update_collections(handler=fileHandler)
        return result, str(err)