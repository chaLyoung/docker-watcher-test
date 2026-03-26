"""TIF → COG 변환 유틸리티"""
import logging
import time
from pathlib import Path

import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logger = logging.getLogger("watcher")


def convert_tif_to_cog(
    input_tif: str,
    output_cog: str | None = None,
    compress: str = "lzw",
    overview_level: int = 4,
) -> str:
    src_path = Path(input_tif)
    if not src_path.exists():
        raise FileNotFoundError(f"Input file not found: {src_path}")

    dst_path = (
        Path(output_cog)
        if output_cog
        else src_path.with_name(f"{src_path.stem}_cog.tif")
    )
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = time.perf_counter()

    profile = cog_profiles.get(compress)
    config = {
        "GDAL_NUM_THREADS": "ALL_CPUS",
        "GDAL_TIFF_INTERNAL_MASK": True,
    }

    with rasterio.open(src_path) as src:
        cog_translate(
            src,
            str(dst_path),
            profile,
            overview_level=overview_level,
            config=config,
            in_memory=False,
            quiet=True,
        )

    elapsed = time.perf_counter() - started_at
    logger.info("[COG] Converted: %s → %s (%.3fs)", src_path.name, dst_path.name, elapsed)
    return str(dst_path)