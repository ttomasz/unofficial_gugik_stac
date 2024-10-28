import http.client
import re
from collections.abc import Generator
from datetime import datetime, timedelta

import geopandas as gpd
import pystac
import pystac.media_type
import pytz
import requests
from owslib.wfs import WebFeatureService, wfs200
from pyproj import Transformer
from requests import Request

from . import const
from .log import logger as package_logger

BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
_service: wfs200.WebFeatureService_2_0_0 | None = None
re_four_digits = re.compile(r"\d{4}")
tz = pytz.timezone("Europe/Warsaw")
crs_names_mapping = {
    "PL-1992": "EPSG:2180",
}
logger = package_logger.getChild(__name__)


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
            logger.info("Connecting to WFS service: %s", BASE_URL)
            return WebFeatureService(url=BASE_URL, version="2.0.0")
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


def get_layer_features(layer_name: str, limit_features: int | None = None, max_retries: int = 5) -> gpd.GeoDataFrame:
    def _try_reading_data(try_number: int = 0) -> gpd.GeoDataFrame:
        try_number += 1
        try:
            return gpd.read_file(wfs_request_url)
        except http.client.RemoteDisconnected as e:
            logger.warning("There was an error when trying to connect to WFS service.")
            if try_number <= max_retries:
                logger.warning("Retrying connection to WFS service (try %s/%s)", try_number, max_retries)
                return _try_reading_data(try_number)
            else:
                raise Exception("Max number of retries reached.") from e
    params = dict(
        service="WFS",
        version="2.0.0",
        request="GetFeature",
        typeName=layer_name,
    )
    if limit_features:
        params["count"] = limit_features
    wfs_request_url = Request('GET', BASE_URL, params=params).prepare().url
    logger.info("Requesting Features from WFS service using URL: %s", wfs_request_url)
    data: gpd.GeoDataFrame = _try_reading_data()
    logger.info("Parsed %s features into GeoDataFrame.", len(data.index))
    return data


def features_as_items(features: gpd.GeoDataFrame) -> Generator[pystac.Item, None, None]:
    logger.debug("Reprojecting features to EPSG:4326.")
    features = features.to_crs(epsg=4326)
    crs_transformer = Transformer.from_crs(2180, 4326)
    for _, feature in features.iterrows():
        # prepare bbox reprojected to EPSG:4326
        xmin, ymin = str(feature["lowerCorner"]).split(" ")
        bottom, left = crs_transformer.transform(float(xmin), float(ymin))
        xmax, ymax = str(feature["upperCorner"]).split(" ")
        top, right = crs_transformer.transform(float(xmax), float(ymax))
        bbox = [left, bottom, right, top]
        logger.debug("Reprojected BBOX from values: %s, %s, %s, %s to: %s", xmin, ymin, xmax, ymax, bbox)

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


def build_ortho_collection() -> pystac.Collection:
    logger.info("Buidling Orthophotomap collection with sub-collections and items...")
    ortho_collection = get_main_collection()
    layers = list(wfs_layers_interator())
    for layer in layers:
        logger.debug("Processing layer: %s(id: %s)...", layer.title, layer.id)
        layer_collection = wfs_layer_as_pystac_collection(layer=layer)
        features = get_layer_features(layer_name=layer.id)
        logger.debug("Adding %s Items to sub-collection %s", len(features.index), layer_collection.id)
        layer_collection.add_items(items=features_as_items(features=features))
        logger.debug("Updating extents of sub-collection. Current value: %s", layer_collection.extent.to_dict())
        layer_collection.update_extent_from_items()
        logger.debug("Finished updating extents of sub-collection. Current value: %s", layer_collection.extent.to_dict())
        ortho_collection.add_child(child=layer_collection)
        logger.debug("Added sub-collection: %s(id: %s) to collection: %s(id: %s).", layer_collection.title, layer_collection.id, ortho_collection.title, ortho_collection.id)
    logger.debug("Updating extents of collection. Current value: %s", ortho_collection.extent.to_dict())
    ortho_collection.update_extent_from_items()
    logger.debug("Finished updating extents of collection. Current value: %s", ortho_collection.extent.to_dict())
    return ortho_collection
