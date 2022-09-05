# Example
# python3 -m matchup "Tag42288_NrM6.csv" "Tag42288_NrM6_eo.csv"

from datetime import timedelta
import numpy as np
from sentinelhub import MimeType, CRS, BBox, SentinelHubRequest, DataCollection, SHConfig
from sentinelhub import SentinelHubCatalog
from datetime import datetime
import pandas as pd
import eo_io
from functools import lru_cache
from dask.distributed import Client
import dask.dataframe as dd
import time
import click

config_sh = SHConfig()
config_s3 = eo_io.configuration()

config_sh.instance_id = config_s3.sh_instance_id
config_sh.sh_client_id = config_s3.sh_client_id
config_sh.sh_client_secret = config_s3.sh_client_secret
config_sh.sh_base_url = 'https://creodias.sentinel-hub.com'

res = 0.0002
buffer_days = 10
cache_size = 1024

evalscript_cloud = """
    //VERSION=3

    function setup() {
        return {
            input: [{
                bands: ["CLM"]
            }],
            output: {
                bands: 1,
                sampleType: SampleType.UINT8
            }
        };
    }

    function evaluatePixel(sample) {
        return [sample.CLM];
    }
"""

# https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/ndmi/#
evalscript_ndmi = """
//VERSION=3
function setup() 
{
    return {input: [{bands: ["B8A", "B11"],}],
            output: {bands: 1, sampleType: SampleType.FLOAT32}}
}

function evaluatePixel(sample) 
{
NDMI = (sample.B8A - sample.B11)/(sample.B8A + sample.B11);
return [NDMI]
}
"""


def wrap(lon):
    return (lon + 180) % (2 * 180) - 180


def lon_lat_to_bbox(lon, lat, resolution):
    """
    lat: latitude / resolution
    lon: longitude / resolution
    """

    return BBox([wrap(lon - resolution / 2),
                 (lat + resolution / 2),
                 wrap(lon + resolution / 2),
                 (lat - resolution / 2)], crs=CRS.WGS84)


date_fornat = "%Y-%m-%dT%H:%M:%SZ"


class SearchCatalogue:

    def __init__(self, resolution, buffer_days=10):
        self.resolution = resolution
        self.buffer_days = buffer_days

        self.catalog = SentinelHubCatalog(config=config_sh)
        self.lon = None
        self.lat = None
        self.datetime_center = None

    # @lru_cache(cache_size)
    def search_catalogue(self, lon, lat, datetime_center):
        """
        lat: latitude / resolution
        lon: longitude / resolution
        """
        sh_bbox = lon_lat_to_bbox(lon, lat, self.resolution)
        start = (datetime_center - timedelta(self.buffer_days)).strftime(date_fornat)
        stop = (datetime_center + timedelta(self.buffer_days)).strftime(date_fornat)
        return list(self.catalog.search(DataCollection.SENTINEL2_L2A,
                                        bbox=sh_bbox,
                                        time=(start, stop),
                                        query={"eo:cloud_cover": {"lt": 100}},
                                        fields={"include": ["id", "properties.datetime", "properties.eo:cloud_cover"],
                                                "exclude": []}))

    def acquisition_time_diff_and_acquisition(self, timestamp: datetime):
        """
        arguments:
            tracking timestamp
            bounding box

        returns:
            absoiute time difference from tracking timestamp and satellite aquisation date
            catalogue result
        """
        # use date (instead of timestamp) in cataluge search so that lru cache works
        search_iterator = self.search_catalogue(self.lon, self.lat, self.datetime_center.date())
        for cat_result in search_iterator:
            acq_time = datetime.strptime(cat_result['properties']['datetime'], date_fornat)
            yield abs(acq_time - timestamp), cat_result

    def get_results(self, timestamp, lon, lat, use_previous_results=False):
        if not use_previous_results:
            self.lon = lon
            self.lat = lat
        self.datetime_center = timestamp
        sorted_res = sorted(self.acquisition_time_diff_and_acquisition(timestamp), key=lambda x: x[0])
        yield from sorted_res


# @lru_cache(cache_size)
def get_request(timestamp, javascript, lon, lat, resolution, size):
    """
    lat: latitude / resolution
    lon: longitude / resolution
    """

    sh_bbox = lon_lat_to_bbox(lon, lat, resolution)
    req = SentinelHubRequest(
        evalscript=javascript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A,
                time_interval=timestamp,
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
        bbox=sh_bbox,
        size=size,
        config=config_sh)
    return req.get_data()


cat = SearchCatalogue(res, buffer_days)


def get_acquisition_time_and_eo_data(timestamp, javascript, lon, lat, size, use_previous_results=False):
    search_iterator = sorted(cat.get_results(timestamp, lon, lat, use_previous_results=use_previous_results),
                             key=lambda x: x[0])
    for search_res in search_iterator:
        acquisition_timestamp = search_res[1]['properties']['datetime']
        data = get_request(acquisition_timestamp, javascript, lon, lat, res, size)
        yield datetime.strptime(acquisition_timestamp, date_fornat), data[0][0][0]


def eo_data(timestamp, lon, lat):
    print(timestamp)
    # Get cloud mask for each satellite acquisition, starting from the nearest in time
    for acq_time, cloud in get_acquisition_time_and_eo_data(timestamp, evalscript_cloud, lon, lat, (1, 1)):
        if not cloud:
            # Return the cloud-free EO data
            eo_res = next(get_acquisition_time_and_eo_data(acq_time, evalscript_ndmi, lon, lat, (1, 1)))
            return eo_res
    else:
        return pd.NaT, np.nan


@click.command()
@click.argument('fname_input')
@click.argument('fname_output')
def main(fname_input, fname_output):
    t1 = time.time()

    df = pd.read_csv(fname_input, parse_dates=[1])
    # df = df.iloc[0:100]
    df = df.set_index('Date_Time')
    df['lon_'] = df['LONG']
    df['lat_'] = df['LAT']

    ddf = dd.from_pandas(df, npartitions=10)
    result = ddf.apply(lambda r: eo_data(r.name, r['lon_'], r['lat_']), axis=1, meta=('datetime64[ns]', np.float32))
    result = result.compute()
    result = pd.DataFrame(result.to_list(), index=result.index, columns=('EO_TimeStamp', 'EO_NDMI'))
    df = df.join(result)

    del df['lon_']
    del df['lat_']
    df['EO_NDMI'] = df['EO_NDMI'].round(decimals=3)
    df.to_csv(fname_output, na_rep='NULL')

    t2 = time.time()
    execution_time = t2 - t1
    print(f'The execution_time was {execution_time:.2f} seconds')


if __name__ == '__main__':
    # With more threads, it is faster, but too many results in throttling
    client = Client(threads_per_worker=2, n_workers=1, dashboard_address='localhost:7744')
    print(client)
    main()
