# /// script
# dependencies = [
#   "xarray @ git+https://github.com/pydata/xarray@main",
#   "zarr @ git+https://github.com/zarr-developers/zarr-python@main",
#   "numpy==2.3.1",
#   "pooch",
#   "netcdf4",
# ]
# ///

import zarr
import xarray as xr

# Store the array in a ZIP file
store = zarr.storage.ZipStore("example-3.zip", mode="w")
ds = xr.tutorial.load_dataset("rasm")
ds.to_zarr(store, mode="w", consolidated=False)

store = zarr.storage.ZipStore("example-3.zip", mode="r")
ds = xr.open_zarr(store, zarr_format=3, consolidated=False)
print(ds)
