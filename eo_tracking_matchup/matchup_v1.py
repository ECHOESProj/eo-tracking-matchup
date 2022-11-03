# Example
# python3 matchup_v1.py "Tag42288_NrM6.csv" "Tag42288_NrM6_eo.csv"

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

RES = 0.0002
RES_CLOUDS = 0.01
BUFFER_DAYS = 5
CACHE_SIZE = 2 ** 14

evalscript_cloud = """
//VERSION=3
function setup() {
    return {
        input: [{
            bands: ["CLM"]
        }],
        output: {
            bands: 2,
            sampleType: SampleType.UINT8
        }
    };
}

function evaluatePixel(sample) {
    return [255, sample.CLM];
}
"""

# https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/ndmi/#
evalscript_ndmi = """
//VERSION=3
function setup() 
{
    return {input: [{bands: ["B8A", "B11", "CLM"],}],
            output: {bands: 2, sampleType: SampleType.FLOAT32}}
}

function evaluatePixel(sample) 
{
NDMI = (sample.B8A - sample.B11)/(sample.B8A + sample.B11);
return [NDMI, sample.CLM]
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

    def search_catalogue(self, datetime_center, lon, lat):
        """
        lat: latitude
        lon: longitude
        """
        sh_bbox = lon_lat_to_bbox(lon, lat, self.resolution)
        start = (datetime_center - timedelta(self.buffer_days)).strftime(date_fornat)
        stop = (datetime_center + timedelta(self.buffer_days)).strftime(date_fornat)
        return self.catalog.search(DataCollection.SENTINEL2_L2A,
                                   bbox=sh_bbox,
                                   time=(start, stop),
                                   query={"eo:cloud_cover": {"lt": 100}},
                                   fields={"include": ["id", "properties.datetime", "properties.eo:cloud_cover"],
                                           "exclude": []})

    def acquisition_time_diff_and_acquisition(self, timestamp: datetime, lon: float, lat: float):
        """
        arguments:
            timestamp: tracking timestamp
            lon: longitude
            lat: latitude

        returns:
            absolute time difference from tracking timestamp and satellite acquisition date
            catalogue result
        """
        # use date (instead of timestamp) in cataluge search so that lru cache works
        search_iterator = self.search_catalogue(timestamp.date(), lon, lat)
        for cat_result in search_iterator:
            acq_time = datetime.strptime(cat_result['properties']['datetime'], date_fornat)
            yield abs(acq_time - timestamp), cat_result

    def get_results(self, timestamp, lon, lat):
        yield from sorted(self.acquisition_time_diff_and_acquisition(timestamp, lon, lat), key=lambda x: x[0])


def get_request(timestamp, javascript, lon, lat, resolution, size):
    """
    lat: latitude
    lon: longitude
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


cat = SearchCatalogue(RES, BUFFER_DAYS)


def get_acquisition_time_and_eo_data(timestamp, javascript, lon, lat, size):
    search_iterator = sorted(cat.get_results(timestamp, lon, lat),
                             key=lambda x: x[0])
    for search_res in search_iterator:
        acquisition_timestamp = search_res[1]['properties']['datetime']
        data = get_request(acquisition_timestamp, javascript, lon, lat, RES, size)
        eo_res, cloud_check = list(data[0][0][0])
        yield datetime.strptime(acquisition_timestamp, date_fornat), eo_res, cloud_check


@lru_cache(CACHE_SIZE)
def get_acquisition_time_and_eo_data_first(*args, **kwargs):
    return next(get_acquisition_time_and_eo_data(*args, **kwargs))


@lru_cache(CACHE_SIZE)
def get_acquisition_time_and_eo_data_cloud(*args, **kwargs):
    return list(get_acquisition_time_and_eo_data(*args, **kwargs))


def eo_data(timestamp, lon, lat):
    print(timestamp)
    # Get cloud mask for each satellite acquisition, starting from the nearest in time
    # Round the inputs to get some speed ut with LRU cache.
    for acq_time, _, cloud in get_acquisition_time_and_eo_data_cloud(timestamp.round('6H'), evalscript_cloud,
                                                                     RES_CLOUDS * (lon // RES_CLOUDS),
                                                                     RES_CLOUDS * (lat // RES_CLOUDS), (1, 1)):
        if not cloud:
            # Return the cloud-free EO data
            timestamp_res, eo_res, cloud_check = \
                get_acquisition_time_and_eo_data_first(acq_time,
                                                       evalscript_ndmi,
                                                       RES * (lon // RES),
                                                       RES * (lat // RES), (1, 1))
            if not cloud_check:
                return timestamp_res, eo_res
    else:
        return pd.NaT, np.nan


@click.command()
@click.argument('fname_input')
@click.argument('fname_output')
def main(fname_input: str, fname_output: str):
    t1 = time.time()

    nrows = 1000  # limit the number of rows for testing
    df = pd.read_csv(fname_input, parse_dates=[8], nrows=nrows, dayfirst=True)
    df = df.set_index('Date_Time')
    df['lon_'] = df['Longitude']
    df['lat_'] = df['Latitude']

    ddf = dd.from_pandas(df, npartitions=100)
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
