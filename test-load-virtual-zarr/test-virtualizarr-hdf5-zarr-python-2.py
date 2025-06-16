# /// script
# dependencies = [
#   "virtualizarr==1.3.2",
#   "fsspec==2025.5.1",
#   "ujson==5.10.0",
#   "h5py==3.14.0",
#   "s3fs==2025.5.1",
#   "xarray==2025.6.1",
#   "matplotlib==3.10.3",
#   "zarr==2.18.7",
#   "numpy==2.3.0",
#   "h5netcdf==1.6.1",
# ]
# ///

import fsspec
import virtualizarr
import xarray as xr
import matplotlib.pyplot as plt

# Input URL to dataset. Note this is a netcdf file stored on s3 (cloud dataset).
url = "s3://carbonplan-share/virtualizarr/local.nc"

so = dict(anon=True, default_fill_cache=False, default_cache_type="first")
output_file = "vz-v2.json"

reader_options = {"storage_options": so, "inline_threshold": 100}
vds = virtualizarr.open_virtual_dataset(url, reader_options=reader_options)
vds.virtualize.to_kerchunk(output_file, format="json")

print("create reference filesystem")
# use fsspec to create filesystem from .json reference file
fs = fsspec.filesystem(
    "reference",
    fo=output_file,
    remote_protocol="s3",
    remote_options=dict(anon=True),
    skip_instance_cache=True,
)

print("open dataset")
# load kerchunked dataset with xarray
ds = xr.open_dataset(
    fs.get_mapper(""), engine="zarr", backend_kwargs={"consolidated": False}
)

print("Plot data")
fig, ax = plt.subplots(figsize=(10, 6))
ds.isel(time=0).air.plot(ax=ax)
fig.savefig("plot-vz-v2.png", dpi=300, bbox_inches="tight")
plt.close(fig)
