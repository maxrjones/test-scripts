# /// script
# dependencies = [
#   "kerchunk==0.2.7",
#   "fsspec==2025.5.1",
#   "ujson==5.10.0",
#   "h5py==3.14.0",
#   "s3fs==2025.5.1",
#   "xarray==2025.6.1",
#   "matplotlib==3.10.3",
#   "zarr==2.18.7",
#   "numpy==2.3.0",
# ]
# ///

import fsspec
import kerchunk.hdf
import ujson
import xarray as xr
import matplotlib.pyplot as plt

so = dict(anon=True, default_fill_cache=False, default_cache_type="first")

# Input URL to dataset. Note this is a netcdf file stored on s3 (cloud dataset).
url = "s3://carbonplan-share/virtualizarr/local.nc"
output_json = "kerchunk-v2.json"
# Uses kerchunk to scan through the netcdf file to create kerchunk mapping and then save output as .json
# Note: In this example, we write the kerchunk output to a .json file.
# You could also keep this information in memory and pass it to fsspec
with fsspec.open(url, **so) as inf:
    print("create kerchunk mapping")
    h5chunks = kerchunk.hdf.SingleHdf5ToZarr(inf, url, inline_threshold=100)
    h5chunks.translate()
    print("write json")
    with open(output_json, "wb") as f:
        f.write(ujson.dumps(h5chunks.translate()).encode())

print("create reference filesystem")
# use fsspec to create filesystem from .json reference file
fs = fsspec.filesystem(
    "reference",
    fo=output_json,
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
fig.savefig("plot-kerchunk-v2.png", dpi=300, bbox_inches="tight")
plt.close(fig)
