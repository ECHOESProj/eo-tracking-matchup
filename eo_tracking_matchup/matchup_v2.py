from os.path import join
from datetime import timedelta
import numpy as np
from sentinelhub import SHConfig
from datetime import datetime
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import eo_io
import re
from sentinelhub import SentinelHubCatalog, DataCollection, SHConfig
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import time
import click
from sentinelhub import CRS, BBox

config_sh = SHConfig()
config_s3 = eo_io.configuration()
config_sh.instance_id = config_s3.sh_instance_id
config_sh.sh_client_id = config_s3.sh_client_id
config_sh.sh_client_secret = config_s3.sh_client_secret

# Your client credentials
client_id = config_sh.sh_client_id
client_secret = config_sh.sh_client_secret

date_fornat = "%Y-%m-%dT%H:%M:%SZ"

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}


def search_catalogue(bbox, start, stop):
    """
    lat: latitude / resolution
    lon: longitude / resolution
    """
    catalog = SentinelHubCatalog(config=config_sh)
    bbox = BBox(bbox, crs=CRS.WGS84)
    result = next(catalog.search(DataCollection.SENTINEL2_L2A,
                                 bbox=bbox,
                                 time=(start, stop),
                                 query={"eo:cloud_cover": {"lt": 100}},
                                 fields={"include": ["id", "properties.datetime", "properties.eo:cloud_cover"],
                                         "exclude": []}))
    return datetime.strptime(result['properties']['datetime'], date_fornat)


evalscript = """
//VERSION=3
function setup() {
  return {
    input: [{
      bands: [
        "CLM",
        "dataMask",
        "B8A", 
        "B11"
        ]
    }],
    output: [
      {
        id: "dataMask",
        bands: 1
      },
      {
        id: "clm",
        bands: 1
      },
      {
        id: "ndmi",
        bands: 1
      }
      ]
  }
}

function evaluatePixel(samples) {
     NDMI = (samples.B8A - samples.B11) / (samples.B8A + samples.B11);

    return {
        dataMask: [samples.dataMask],
        clm: [samples.CLM],
        ndmi: [NDMI]
        }
}
"""


def stats_request(bbox, start, stop):
    return {
        "input": {
            "bounds": {
                "bbox": bbox,
                "properties": {
                    "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                }
            },
            "data": [
                {
                    "type": "sentinel-2-l2a",
                    "dataFilter": {
                        "mosaickingOrder": "leastRecent"
                    }
                }
            ]
        },
        "aggregation": {
            "timeRange": {
                "from": start,
                "to": stop
            },
            "aggregationInterval": {
                "of": "P1D"
            },
            "evalscript": evalscript,
            "resx": 1,
            "resy": 1
        }
    }


def get_request(stats_request_json):
    url = "https://services.sentinel-hub.com/api/v1/statistics"
    # Create a session
    oauth = OAuth2Session(client=BackendApplicationClient(client_id=client_id))

    # Get token for the session
    token = oauth.fetch_token(token_url='https://services.sentinel-hub.com/oauth/token',
                              client_secret=client_secret)
    # All requests using this session will have an access token automatically added
    resp = oauth.get("https://services.sentinel-hub.com/oauth/tokeninfo")

    response = oauth.request("POST", url=url, headers=headers, json=stats_request_json)
    return response.json()


def get_cloud_free(bbox, datetime_center, buffer_days=10):
    start = (datetime_center - timedelta(days=buffer_days)).strftime(date_fornat)
    stop = (datetime_center + timedelta(days=buffer_days)).strftime(date_fornat)
    start = re.sub('\d{2}:\d{2}:\d{2}Z', '00:00:00Z', start)
    stop = re.sub('\d{2}:\d{2}:\d{2}Z', '00:00:00Z', stop)
    return get_request(stats_request(bbox, start, stop))


def info(r, v):
    return r['outputs'][v]['bands']['B0']['stats']


def wrap(lon):
    return (lon + 180) % (2 * 180) - 180


def matchup(timestamp, lon, lat):
    print(timestamp)
    resolution = 0.0002
    bbox = [wrap(lon - resolution / 2),
            (lat + resolution / 2),
            wrap(lon + resolution / 2),
            (lat - resolution / 2)]

    try:
        result = get_cloud_free(bbox, timestamp)
        ts_val = [(search_catalogue(bbox, r['interval']['from'], r['interval']['to']), info(r, 'ndmi')['max'])
                  for r in result['data'] if info(r, 'clm')['mean'] == 0]
        delta_val = [(np.abs(ts - timestamp), ts, val) for (ts, val) in ts_val]
        return sorted(delta_val, key=lambda x: x[0])[0][1:]
    except (IndexError, KeyError):
        return pd.NaT, np.nan


@click.command()
@click.argument('fname_input')
@click.argument('fname_output')
def main(fname_input, fname_output):
    t1 = time.time()

    df = pd.read_csv(fname_input, parse_dates=[1])
    df = df.iloc[0:100]
    df = df.set_index('Date_Time')
    df['lon_'] = df['LONG']
    df['lat_'] = df['LAT']

    # Pandas (no threading)
    # result = df.apply(lambda r: matchup(r.name, r['lon_'], r['lat_']), axis=1)
    # result = pd.DataFrame(result.to_list(), index=df.index, columns=('EO_TimeStamp', 'EO_NDMI'))
    # df = df.join(result)

    # dask (threading)
    ddf = dd.from_pandas(df, npartitions=2)
    result = ddf.apply(lambda r: matchup(r.name, r['lon_'], r['lat_']), axis=1, meta=('datetime64[ns]', np.float32))
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
