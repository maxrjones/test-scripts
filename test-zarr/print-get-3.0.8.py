# /// script
# dependencies = [
#   "xarray @ git+https://github.com/pydata/xarray@main",
#   "zarr @ git+https://github.com/maxrjones/zarr-python@3.0.8-with-print",
#   "numpy==2.3.1",
#   "pooch",
#   "netcdf4",
# ]
# ///

import zarr
from zarr.storage import MemoryStore
import xarray as xr

print(zarr.__version__)
original = xr.Dataset({"foo": ("x", [1])}, coords={"x": [0]})

with MemoryStore() as store:
    original.to_zarr(store)
