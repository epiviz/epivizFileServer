import requests
# import umsgpack
import ujson

from ..measurements import MeasurementManager, WebServerMeasurement

class EpivizClient(object):
    """
    Client implementation of the epiviz server

    Args:
        server: endpoint where the API is running
    """

    version = 5

    def __init__(self, server):
        self.server = server
        self.requestId = 1
        self.measurements = []

    def get_measurements(self):
        params = {
            'requestId': self.requestId,
            'version': self.version,
            'action': 'getMeasurements'
        }

        self.requestId += 1
        res = requests.get(self.server, params=params)

        # result = umsgpack.unpackb(res.content)
        result = res.content
        data = result['data']
        
        for i in range(len(data['id'])):
            self.measurements.append(
                WebServerMeasurement(data['type'][i], data['id'][i], data['name'][i], self.server, 
                   data['datasourceId'][i], data['datasourceGroup'][i], data['annotation'][i], data['metadata'][i]
                )
            )

        return self.measurements

    def get_seq_info(self):
        params = {
            'requestId': self.requestId,
            'version': self.version,
            'action': 'getSeqInfos',
            'datasourceGroup': self.sname
        }

        self.requestId += 1
        res = requests.get(self.server, params=params)
        # result = umsgpack.unpackb(res.content)
        result = res.content
        return result

    def get_data(self, measurement, chr, start, end):
        """Get data for a genomic region from the API

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end

        Returns:
            a json with results
        """

        result = measurement.get_data(chr, start, end, requestId=self.requestId )
        self.requestId += 1

        return result