import asyncio
import logging
import tempfile
from collections.abc import Iterable
from pathlib import Path
from xml.dom import pulldom

import geopandas as gpd
import pandas as pd

from ggkstac.ortho.const import WFS_BASE_URL
from ggkstac.utils import download

logger = logging.getLogger(__name__)


async def download_and_parse_layer(layer_id: str, page_size: int = 20_000) -> gpd.GeoDataFrame:
    logger.info("Downloading data for layer: %s...", layer_id)
    params = dict(
        SERVICE="WFS",
        VERSION="2.0.0",
        REQUEST="GetFeature",
        SRSNAME="EPSG:4326",
        TYPENAME=layer_id,
        COUNT=page_size,
    )
    dfs: list[gpd.GeoDataFrame] = []
    async def _go_through_pages(url: str, params: dict | None) -> None:
        with tempfile.NamedTemporaryFile() as f:
            await download(url=url, params=params, file_path=f.name)
            dfs.append(gpd.read_file(f))
            f.seek(0)
            next_url = None
            doc = pulldom.parse(f)
            for event, node in doc:
                if event == pulldom.START_ELEMENT and node.tagName == "wfs:FeatureCollection":
                    next_url = node.getAttribute('next')
                    break
            if next_url:
                await _go_through_pages(url=next_url, params=None)

    await _go_through_pages(url=WFS_BASE_URL, params=params)
    data = pd.concat(dfs)
    logger.info("Finished downloading data for layer: %s. Number of rows: %s.", layer_id, len(data.index))
    return data


async def download_and_save_to_geoparquet(
    layer_ids: Iterable[str],
    output_directory: str | Path,
    max_workers: int = 3,
    download_kwargs: dict | None = None,
    save_kwargs: dict | None = None,
) -> list[Path]:
    semaphore = asyncio.Semaphore(max_workers)
    folder = output_directory if isinstance(output_directory, Path) else Path(output_directory)
    if not folder.exists():
        folder.mkdir(parents=True)
    logger.info("Output directory is: %s", folder.absolute().resolve())
    download_kwargs = download_kwargs or {}
    save_kwargs = save_kwargs or {}

    async def worker(layer_id: str) -> Path:
        async with semaphore:
            file_path = folder / f"{layer_id}.parquet"
            df = await download_and_parse_layer(layer_id=layer_id, **download_kwargs)
            logger.info("Saving GeoDataFrame with layer's: %s data to geoparquet: %s", layer_id, file_path)
            df.to_parquet(file_path, index=False, compression="zstd", **save_kwargs)
            logger.info("Finished saving geoparquet for layer: %s to geopaquet file: %s", layer_id, file_path)
            return file_path

    return await asyncio.gather(*[worker(layer_id) for layer_id in layer_ids])
