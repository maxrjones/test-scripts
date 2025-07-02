# /// script
# dependencies = [
#   "xarray @ git+https://github.com/pydata/xarray@main",
#   "zarr == 3.0.9",
#   "numpy==2.3.1",
#   "pooch",
#   "netcdf4",
# ]
# ///

from zarr.storage import MemoryStore, LoggingStore
import xarray as xr
import logging

log_filename = "zarr_3.0.9.log"

original = xr.Dataset({"foo": ("x", [1])}, coords={"x": [0]})
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
    ],
)

with LoggingStore(MemoryStore()) as store:
    original.to_zarr(store)
