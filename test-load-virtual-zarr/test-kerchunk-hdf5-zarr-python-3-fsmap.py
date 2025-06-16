# /// script
# dependencies = [
#   "kerchunk==0.2.8",
#   "fsspec==2025.5.1",
#   "ujson==5.10.0",
#   "h5py==3.14.0",
#   "s3fs==2025.5.1",
#   "xarray==2025.6.1",
#   "matplotlib==3.10.3",
#   "zarr @ git+https://github.com/zarr-developers/zarr-python@main",
#   "h5netcdf==1.6.1",
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
output_json = "kerchunk-v3.json"
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

# Traceback (most recent call last):
#   File "/Users/max/Documents/Code/maxrjones/test-scripts/test-virtual-zarr/test-kerchunk-hdf5-zarr-python-fsmap.py", line 49, in <module>
#     ds = xr.open_dataset(
#          ^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/xarray/backends/api.py", line 687, in open_dataset
#     backend_ds = backend.open_dataset(
#                  ^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/xarray/backends/zarr.py", line 1578, in open_dataset
#     store = ZarrStore.open_group(
#             ^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/xarray/backends/zarr.py", line 664, in open_group
#     ) = _get_open_params(
#         ^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/xarray/backends/zarr.py", line 1815, in _get_open_params
#     zarr_group = zarr.open_group(store, **open_kwargs)
#                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/_compat.py", line 43, in inner_f
#     return f(*args, **kwargs)
#            ^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/api/synchronous.py", line 535, in open_group
#     sync(
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/core/sync.py", line 163, in sync
#     raise return_result
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/core/sync.py", line 119, in _runner
#     return await coro
#            ^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/api/asynchronous.py", line 837, in open_group
#     store_path = await make_store_path(store, mode=mode, storage_options=storage_options, path=path)
#                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/storage/_common.py", line 333, in make_store_path
#     store = FsspecStore.from_mapper(store_like, read_only=_read_only)
#             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/storage/_fsspec.py", line 205, in from_mapper
#     fs = _make_async(fs_map.fs)
#          ^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/zarr/storage/_fsspec.py", line 57, in _make_async
#     return fsspec.AbstractFileSystem.from_json(json.dumps(fs_dict))
#            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/fsspec/spec.py", line 1480, in from_json
#     return json.loads(blob, cls=FilesystemJSONDecoder)
#            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.local/share/uv/python/cpython-3.11.10-macos-aarch64-none/lib/python3.11/json/__init__.py", line 359, in loads
#     return cls(**kw).decode(s)
#            ^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.local/share/uv/python/cpython-3.11.10-macos-aarch64-none/lib/python3.11/json/decoder.py", line 337, in decode
#     obj, end = self.raw_decode(s, idx=_w(s, 0).end())
#                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.local/share/uv/python/cpython-3.11.10-macos-aarch64-none/lib/python3.11/json/decoder.py", line 353, in raw_decode
#     obj, end = self.scan_once(s, idx)
#                ^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/fsspec/json.py", line 101, in custom_object_hook
#     return AbstractFileSystem.from_dict(dct)
#            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/fsspec/spec.py", line 1556, in from_dict
#     return cls(
#            ^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/fsspec/spec.py", line 81, in __call__
#     obj = super().__call__(*args, **kwargs)
#           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/max/.cache/uv/environments-v2/test-kerchunk-hdf5-zarr-python-fsmap-d92e07cb672c357b/lib/python3.11/site-packages/fsspec/implementations/reference.py", line 770, in __init__
#     raise ValueError(
# ValueError: Reference-FS's target filesystem must have same valueof asynchronous
print("Plot data")
fig, ax = plt.subplots(figsize=(10, 6))
ds.isel(time=0).air.plot(ax=ax)
fig.savefig("plot-kerchunk-v3.png", dpi=300, bbox_inches="tight")
plt.close(fig)
