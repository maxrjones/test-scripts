# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "morecantile",
#     "rasterio",
# ]
# ///

import morecantile
import rasterio
import rasterio.warp

tms = morecantile.tms.get("WebMercatorQuad")


def warp_bounds(x: int, y: int, z: int) -> None:
    bounds = tms.xy_bounds(x, y, z)
    transformed = rasterio.warp.transform_bounds("EPSG:3857", "EPSG:4326", *bounds)

    print()
    print(f"tile: {(x, y, z)}")
    print(f"bounds: {bounds}")
    print(f"transformed: {transformed}")


def main() -> None:
    warp_bounds(0, 0, 0)
    warp_bounds(-1, 0, 0)
    warp_bounds(-2, 0, 0)
    warp_bounds(1, 0, 0)
    warp_bounds(2, 0, 0)


# tile: (0, 0, 0)
# bounds: BoundingBox(left=-20037508.342789244, bottom=-20037508.34278925, right=20037508.34278925, top=20037508.342789244)
# transformed: (-180.0, -85.0511287798066, 180.0, 85.05112877980659)

# tile: (-1, 0, 0)
# bounds: BoundingBox(left=-60112525.02836774, bottom=-20037508.34278925, right=-20037508.342789244, top=20037508.342789244)
# transformed: (-180.0, -85.0511287798066, 180.0, 85.05112877980659)

# tile: (-2, 0, 0)
# bounds: BoundingBox(left=-100187541.71394624, bottom=-20037508.34278925, right=-60112525.02836774, top=20037508.342789244)
# transformed: (179.9999999999998, -85.0511287798066, 179.9999999999999, 85.05112877980659)

# tile: (1, 0, 0)
# bounds: BoundingBox(left=20037508.34278925, bottom=-20037508.34278925, right=60112525.02836774, top=20037508.342789244)
# transformed: (-180.0, -85.0511287798066, 180.0, 85.05112877980659)

# tile: (2, 0, 0)
# bounds: BoundingBox(left=60112525.02836774, bottom=-20037508.34278925, right=100187541.71394624, top=20037508.342789244)
# transformed: (-179.9999999999999, -85.0511287798066, -179.9999999999998, 85.05112877980659)


if __name__ == "__main__":
    main()
