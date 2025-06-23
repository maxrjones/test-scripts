# /// script
# dependencies = [
#   "fsspec==2025.5.1",
#   "h5py==3.14.0",
#   "s3fs==2025.5.1",
#   "xarray==2025.6.1",
#   "numpy==2.3.0",
#   "earthaccess==0.14.0",
#   "h5netcdf==1.6.1",
#   "dask",
# ]
# ///

# Note: this script needs to be run from the VEDA science hub environment for the credentials to work.

import earthaccess
import datetime
import fsspec
import pandas as pd
import xarray as xr

start_date = datetime.datetime(2022, 4, 1)
end_date = datetime.datetime(2022, 5, 12)
date_array = pd.date_range(start=start_date, end=end_date, freq="D").to_pydatetime()

short_name = "GLDAS_NOAH025_3H"
version = "2.1"
variable = "SoilMoi0_10cm_inst"  # Only select a single variable of interest

print("Retrieving data granules from Earthaccess")
earthaccess.login()
results = earthaccess.search_data(
    short_name=short_name,
    version=version,
    temporal=(start_date, end_date),
    cloud_hosted=True,
)

# grab the S3 URLs
urls = [g["umm"]["RelatedUrls"][1]["URL"] for g in results]
print(f"Found {len(urls)} files")
fs = fsspec.filesystem("s3")
fsspec_caching = {"cache_type": "mmap"}
lat_range = slice(24, 50)
lon_range = slice(-125, -66)


def subset(ds):
    return ds[[variable]].sel(lat=lat_range, lon=lon_range)


ds = xr.open_mfdataset(
    [fs.open(url, **fsspec_caching) for url in urls],
    chunks="auto",
    concat_dim="time",
    combine="nested",
    parallel=True,
    data_vars="minimal",
    coords="minimal",
    compat="override",
    join="exact",
    preprocess=subset,
)
ds_resampled = ds.resample(time="1D").mean(dim="time").compute()
ds_resampled
