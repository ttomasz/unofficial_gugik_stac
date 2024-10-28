import asyncio
import logging
import re
import tempfile
from collections.abc import Generator, Iterable
from datetime import datetime, timedelta

import geopandas as gpd
import pystac
import pystac.media_type
import pytz
import requests
from owslib.wfs import WebFeatureService, wfs200

from ggkstac.utils import download

from . import const

WFS_BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
_service: wfs200.WebFeatureService_2_0_0 | None = None
re_four_digits = re.compile(r"\d{4}")
tz = pytz.timezone("Europe/Warsaw")
crs_names_mapping = {
    "PL-1992": "EPSG:2180",
}
logger = logging.getLogger(__name__)


def get_main_collection() -> pystac.Collection:
    logger.info("Creating main Orthophotomap collection.")
    return pystac.Collection(
        id=const.ID_COLLECTION_ORTHO,
        title="Ortofotomapy",
        description="Kolekcja z arkuszami ortofotomap, które można pobrać. Podzielona latami.",
        extent=pystac.Extent(  # initial extent, to be updated after adding objects
            spatial=pystac.SpatialExtent(bboxes=[const.BBOX_POLAND]),
            temporal=pystac.TemporalExtent(intervals=[datetime(1957, 1, 1), datetime.today()])
        ),
        license=const.CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
    )


def wfs_service(max_retries: int = 5) -> wfs200.WebFeatureService_2_0_0:
    global _service
    if _service is not None:
        return _service
    
    def _get_service(try_number: int = 0) -> wfs200.WebFeatureService_2_0_0:
        try_number += 1
        try:
            logger.info("Connecting to WFS service: %s", WFS_BASE_URL)
            return WebFeatureService(url=WFS_BASE_URL, version="2.0.0")
        except requests.exceptions.ConnectionError as e:
            logger.warning("There was an error when trying to connect to WFS service.")
            if try_number <= max_retries:
                logger.warning("Retrying connection to WFS service (try %s/%s)", try_number, max_retries)
                return _get_service(try_number)
            else:
                raise Exception("Max number of retries reached.") from e

    _service = _get_service()

    return _service


def wfs_layers_interator() -> Generator[wfs200.ContentMetadata, None, None]:
    logger.info("Listing WFS layers...")
    service = wfs_service()
    content = service.contents
    yield from content.values()


def wfs_layer_as_pystac_collection(layer: wfs200.ContentMetadata) -> pystac.Collection:
    logger.info("Converting layer: %s(id: %s) to PySTAC collection.", layer.title, layer.id)
    year_match = re_four_digits.search(layer.title)
    if year_match is None:
        raise Exception(f"Could not find year in WFS layer title: {layer.title}")
    year = int(year_match.group())
    start_dt = datetime(year=year, month=1, day=1, tzinfo=tz)
    end_dt = datetime(year=year + 1, month=1, day=1, tzinfo=tz) - timedelta(microseconds=1)
    collection = pystac.Collection(
        id=const.ID_SUBCOLLECTION_ORTHO_TEMPLATE.format(year=str(year)),
        title=str(year),
        description=f"Arkusze ortofotomapy z roku: {year}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[layer.boundingBoxWGS84]),
            temporal=pystac.TemporalExtent(intervals=[[start_dt, end_dt]]),
        ),
        license=const.CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
    )
    logger.debug("Created Collection: %s", collection)
    return collection


def features_as_items(features: gpd.GeoDataFrame) -> Generator[pystac.Item, None, None]:
    for _, feature in features.iterrows():
        ymin, xmin = str(feature["lowerCorner"]).split(" ")
        ymax, xmax = str(feature["upperCorner"]).split(" ")
        bbox = [float(xmin), float(ymin), float(xmax), float(ymax)]

        item = pystac.Item(
            id=feature["gml_id"],
            geometry=feature["geometry"].__geo_interface__,
            bbox=bbox,
            datetime=datetime.fromisoformat(feature["timePosition"]),
            properties={},
            assets={
                "image": pystac.Asset(
                    href=feature["url_do_pobrania"],
                    media_type=pystac.media_type.MediaType.GEOTIFF,
                    roles=["data"],
                ),
            },
        )
        logger.debug("Created Item: %s", item)
        yield item


async def prepare_subcollection(layer: wfs200.ContentMetadata) -> pystac.Collection:
    logger.debug("Processing layer: %s(id: %s)...", layer.title, layer.id)
    params = dict(
        SERVICE="WFS",
        VERSION="2.0.0",
        REQUEST="GetFeature",
        SRSNAME="EPSG:4326",
        TYPENAME=layer.id,
    )
    layer_collection = wfs_layer_as_pystac_collection(layer=layer)
    with tempfile.NamedTemporaryFile() as f:
        await download(url=WFS_BASE_URL, params=params, file_path=f.name)
        data: gpd.GeoDataFrame = gpd.read_file(f)
        logger.info("[Layer: %s] Parsed %s features into GeoDataFrame.", layer.id, len(data.index))
        logger.debug("[Layer: %s] Adding Items to sub-collection %s", layer.id, layer_collection.id)
        layer_collection.add_items(items=features_as_items(features=data))
        logger.debug("[Layer: %s] Updating extents of sub-collection. Current value: %s", layer.id, layer_collection.extent.to_dict())
        layer_collection.update_extent_from_items()
        logger.debug("[Layer: %s] Finished updating extents of sub-collection. Current value: %s", layer.id, layer_collection.extent.to_dict())
    return layer_collection


async def prepare_in_parallel(layers: Iterable[wfs200.ContentMetadata], max_workers: int = 3) -> list[pystac.Collection]:
    semaphore = asyncio.Semaphore(max_workers)

    async def worker(layer: wfs200.ContentMetadata):
        async with semaphore:
            return await prepare_subcollection(layer=layer)

    return await asyncio.gather(*[worker(layer) for layer in layers])


async def build_ortho_collection() -> pystac.Collection:
    logger.info("Buidling Orthophotomap collection with sub-collections and items...")
    ortho_collection = get_main_collection()
    subcollections = await prepare_in_parallel(layers=wfs_layers_interator())
    logger.info("Finished preparing sub-collections. Adding to Main Ortho collection.")
    ortho_collection.add_children(children=subcollections)
    logger.debug("Updating extents of collection. Current value: %s", ortho_collection.extent.to_dict())
    ortho_collection.update_extent_from_items()
    logger.debug("Finished updating extents of collection. Current value: %s", ortho_collection.extent.to_dict())
    return ortho_collection
