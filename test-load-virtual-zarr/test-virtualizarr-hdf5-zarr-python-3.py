# /// script
# dependencies = [
#   "virtualizarr @ git+https://github.com/zarr-developers/VirtualiZarr@refactor/backends",
#   "fsspec==2025.5.1",
#   "ujson==5.10.0",
#   "h5py==3.14.0",
#   "s3fs==2025.5.1",
#   "xarray==2025.6.1",
#   "matplotlib==3.10.3",
#   "zarr==3.0.8",
#   "h5netcdf==1.6.1",
# ]
# ///

import obstore
import virtualizarr
import xarray as xr
import matplotlib.pyplot as plt
import os

# Input URL to dataset. Note this is a netcdf file stored on s3 (cloud dataset).
url = "s3://carbonplan-share/virtualizarr/local.nc"
reference_json = "vz-v3.json"
store = obstore.store.S3Store(
    bucket="carbonplan-share",
    skip_signature=True,
    region="us-west-2",
)

with virtualizarr.open_virtual_dataset(
    url,
    object_store=store,
    parser=virtualizarr.parsers.HDFParser(),
    loadable_variables=[],
) as vds:
    vds.virtualize.to_kerchunk(reference_json, format="json")

registry = virtualizarr.manifests.ObjectStoreRegistry({url: store})
json_parser = virtualizarr.parsers.KerchunkJSONParser(
    store_registry=registry,
)
manifest_store = json_parser(
    reference_json, obstore.store.LocalStore(prefix=os.getcwd())
)
print("open dataset")
ds = xr.open_zarr(manifest_store, consolidated=False)

# Traceback (most recent call last):
#   File "/Users/max/Documents/Code/maxrjones/test-scripts/test-load-virtual-zarr/test-virtualizarr-hdf5-zarr-python-3.py", line 46, in <module>
#     ds = xr.open_zarr(manifest_store, consolidated=False)
#          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/xarray/backends/zarr.py", line 1505, in open_zarr
#     ds = open_dataset(
#          ^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/xarray/backends/api.py", line 687, in open_dataset
#     backend_ds = backend.open_dataset(
#                  ^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/xarray/backends/zarr.py", line 1578, in open_dataset
#     store = ZarrStore.open_group(
#             ^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/xarray/backends/zarr.py", line 664, in open_group
#     ) = _get_open_params(
#         ^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/xarray/backends/zarr.py", line 1815, in _get_open_params
#     zarr_group = zarr.open_group(store, **open_kwargs)
#                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/zarr/_compat.py", line 43, in inner_f
#     return f(*args, **kwargs)
#            ^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/zarr/api/synchronous.py", line 529, in open_group
#     sync(
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/zarr/core/sync.py", line 163, in sync
#     raise return_result
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/zarr/core/sync.py", line 119, in _runner
#     return await coro
#            ^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-virtualizarr-hdf5-zarr-python-3-9a53e0343ebe0f09/lib/python3.11/site-packages/zarr/api/asynchronous.py", line 839, in open_group
#     raise FileNotFoundError(f"Unable to find group: {store_path}")
# FileNotFoundError: Unable to find group: ManifestStore(group=
# ManifestGroup(
#     arrays={'air': ManifestArray<shape=(2920, 25, 53), dtype=int16, chunks=(2920, 25, 53)>, 'lat': ManifestArray<shape=(25,), dtype=float32, chunks=(25,)>, 'lon': ManifestArray<shape=(53,), dtype=float32, chunks=(53,)>, 'time': ManifestArray<shape=(2920,), dtype=float32, chunks=(2920,)>},
#     groups={},
#     metadata=GroupMetadata(attributes={'Conventions': 'COARDS', 'title': '4x daily NMC reanalysis (1948)', 'description': 'Data is from NMC initialized reanalysis\n(4x/day).  These are the 0.9950 sigma level values.', 'platform': 'Model', 'references': 'http://www.esrl.noaa.gov/psd/data/gridded/data.ncep.reanalysis.html'}, zarr_format=3, consolidated_metadata=None, node_type='group'),
# )
# , stores=<virtualizarr.manifests.store.ObjectStoreRegistry object at 0x12b60e5d0>)
print("Plot data")
fig, ax = plt.subplots(figsize=(10, 6))
ds.isel(time=0).air.plot(ax=ax)
fig.savefig("plot-vz-v3.png", dpi=300, bbox_inches="tight")
plt.close(fig)
