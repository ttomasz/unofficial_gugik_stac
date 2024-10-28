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

from .log import logger as package_logger

BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
CC0 = "CC0-1.0"
BASE_ID = "poland.gugik.orto"
BBOX_POLAND = [14.0745211117, 49.0273953314, 24.0299857927, 54.8515359564]
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
        id=BASE_ID,
        title="Ortofotomapy",
        description="",
        extent=pystac.Extent(  # initial extent, to be updated after adding objects
            spatial=pystac.SpatialExtent(bboxes=[BBOX_POLAND]),
            temporal=pystac.TemporalExtent(intervals=[datetime(1957, 1, 1), datetime.today()])
        ),
        license=CC0,
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
        id=f"{BASE_ID}.{layer.id}",
        title=layer.title,  # todo: maybe construct name by myself from year to allow for transaltions?
        description=layer.title,
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[layer.boundingBoxWGS84]),
            temporal=pystac.TemporalExtent(intervals=[[start_dt, end_dt]]),
        ),
        license=CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
    )
    logger.debug("Created Collection: %s", collection)
    return collection


def get_layer_features(layer_name: str) -> gpd.GeoDataFrame:
    params = dict(
        service="WFS",
        version="2.0.0",
        request="GetFeature",
        typeName=layer_name,
        count=10,
    )
    wfs_request_url = Request('GET', BASE_URL, params=params).prepare().url
    logger.info("Requesting Features from WFS service using URL: %s", wfs_request_url)
    data: gpd.GeoDataFrame = gpd.read_file(wfs_request_url)
    logger.info("Parsed %s features into GeoDataFrame.", len(data.index))
    return data


def features_as_items(features: gpd.GeoDataFrame) -> Generator[pystac.Item, None, None]:
    logger.debug("Reprojecting features to EPSG:4326.")
    features = features.to_crs(epsg=4326)
    crs_transformer = Transformer(2180, 4326)
    for _, feature in features.iterrows():
        # prepare bbox reprojected to EPSG:4326
        xmin, ymin = str(feature["lowerCorner"]).split(" ")
        left, bottom = crs_transformer.transform(float(xmin), float(ymin))
        xmax, ymax = str(feature["upperCorner"]).split(" ")
        right, top = crs_transformer.transform(float(xmax), float(ymax))
        bbox = (left, bottom, right, top)
        logger.debug("Reprojected BBOX from values: %s, %s, %s, %s to: %s", xmin, ymin, xmax, ymax, bbox)

        item = pystac.Item(
            id=feature["gml_id"],
            geometry=bbox,
            bbox=feature["geometry"],
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
        # feature["gml_id"]
        # feature["lowerCorner"]
        # feature["upperCorner"]
        feature["godlo"]
        feature["akt_rok"]
        feature["piksel"]
        feature["kolor"]  # "B/W", "RGB", "CIR"
        feature["zrodlo_danych"]  # "Zdj. analogowe", "Zdj. cyfrowe"
        feature["uklad_xy"]  # "PL-1992"
        feature["modul_archiwizacji"]
        feature["nr_zglosz"]
        feature["timePosition"]  # akt_data
        feature["czy_ark_wypelniony"]  # "TAK", "NIE"
        feature["url_do_pobrania"]
        feature["dt_pzgik|timePosition"]
        feature["wlk_pliku_MB"]
        feature["geometry"]
