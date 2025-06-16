# /// script
# dependencies = [
#   "virtualizarr @ git+https://github.com/zarr-developers/VirtualiZarr@develop",
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
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser, KerchunkJSONParser
from virtualizarr.manifests import ObjectStoreRegistry
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

with open_virtual_dataset(
    url,
    object_store=store,
    parser=HDFParser(),
    loadable_variables=[],
) as vds:
    vds.virtualize.to_kerchunk(reference_json, format="json")

registry = ObjectStoreRegistry({url: store})
json_parser = KerchunkJSONParser(
    store_registry=registry,
)
manifest_store = json_parser(
    reference_json, obstore.store.LocalStore(prefix=os.getcwd())
)
print("open dataset")
ds = xr.open_zarr(manifest_store, consolidated=False, zarr_format=3)

print("Plot data")
fig, ax = plt.subplots(figsize=(10, 6))
ds.isel(time=0).air.plot(ax=ax)
fig.savefig("plot-vz-v3.png", dpi=300, bbox_inches="tight")
plt.close(fig)
