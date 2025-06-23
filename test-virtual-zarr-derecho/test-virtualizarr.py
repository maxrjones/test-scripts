# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "obstore",
#     "virtualizarr[hdf]",
#     "earthaccess",
# ]
#
# [tool.uv.sources]
# obstore = { git = "https://github.com/developmentseed/obstore.git", subdirectory = "obstore", rev = "50782ed782a15185a936d435d13ca0a7969154ae" }
# virtualizarr = { git = "https://github.com/zarr-developers/VirtualiZarr.git", branch = "develop" }
# ///

from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from virtualizarr.parsers import HDFParser
from virtualizarr import open_virtual_dataset
import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

data_url = "s3://gesdisc-cumulus-prod-protected/GLDAS/GLDAS_NOAH025_3H.2.1/2022/091/GLDAS_NOAH025_3H.A20220401.0000.021.nc4"
data_prefix_url, filename = data_url.rsplit("/", 1)
print(data_prefix_url)
print(filename)
cp = NasaEarthdataCredentialProvider(
    "https://data.gesdisc.earthdata.nasa.gov/s3credentials",
)
store = S3Store.from_url(data_prefix_url, credential_provider=cp)
ds = open_virtual_dataset(data_url, object_store=store, parser=HDFParser())
print(ds)
