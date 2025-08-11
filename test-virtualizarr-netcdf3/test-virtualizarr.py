# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "obstore",
#     "virtualizarr[netcdf3, icechunk]",
#     "earthaccess",
#     "dask",
# ]
#
# [tool.uv.sources]
# virtualizarr = { git = "https://github.com/maxrjones/VirtualiZarr.git", branch = "kerchunk-error" }
# ///


import xarray as xr

from obstore.store import S3Store

from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry

import virtualizarr


print(virtualizarr.__version__)

endpoint = "https://usgs.osn.mghpcc.org"
bucket = "esip"
prefix = "rsignell/testing/shyfem"
path = f"{bucket}/{prefix}"
file = "nos00.nc"
scheme = "s3://"

# Create the S3Store
store = S3Store.from_url(
    f"{scheme}{path}", endpoint=endpoint, skip_signature=True, region="us-east-1"
)

# Setup a registry, which is used for any loadable variables
registry = ObjectStoreRegistry({f"{scheme}{path}": store})

# NetCDF3Parser is a light wrapper around kerchunk.netCDF3.NetCDF3ToZarr
# Using `reader_options={"storage_options": storage_options}` when creating a NetCDF3Parser instance is the same as NetCDF3ToZarr(url, inline_threshold=0, **storage_options)
s3fs_opts = {"anon": True, "endpoint_url": endpoint}
parser = NetCDF3Parser(reader_options={"storage_options": s3fs_opts})

vds = open_virtual_dataset(
    url=f"{scheme}{path}/{file}", registry=registry, parser=parser
)

# Kerchunk

vds.virtualize.to_kerchunk("test.json", format="json")

so = dict(anon=True, endpoint_url="https://usgs.osn.mghpcc.org")
ds = xr.open_dataset(
    "test.json",
    engine="kerchunk",
    chunks={"time": 1},
    backend_kwargs=dict(storage_options=dict(remote_protocol="s3", remote_options=so)),
)

print(ds["salinity"][0, 0, :4].values)
