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

BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
CC0 = "CC0-1.0"
BASE_ID = "poland.gugik.orto"
_service: wfs200.WebFeatureService_2_0_0 | None = None
re_four_digits = re.compile(r"\d{4}")
tz = pytz.timezone("Europe/Warsaw")
crs_names_mapping = {
    "PL-1992": "EPSG:2180",
}


def get_main_collection() -> pystac.Collection:
    return pystac.Collection(
        id=BASE_ID,
        title="Ortofotomapy",
        description="",
    )


def wfs_service(max_retries: int = 5) -> wfs200.WebFeatureService_2_0_0:
    global _service
    if _service is not None:
        return _service
    
    def _get_service(try_number: int = 0) -> wfs200.WebFeatureService_2_0_0:
        try_number += 1
        try:
            return WebFeatureService(url=BASE_URL, version="2.0.0")
        except requests.exceptions.ConnectionError as e:
            if try_number <= max_retries:
                return _get_service(try_number)
            else:
                raise Exception("Max number of retries reached.") from e

    _service = _get_service()

    return _service


def wfs_layers_interator() -> Generator[wfs200.ContentMetadata, None, None]:
    service = wfs_service()
    content = service.contents
    yield from content.values()


def wfs_layer_as_pystac_collection(layer: wfs200.ContentMetadata) -> pystac.Collection:
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
    data: gpd.GeoDataFrame = gpd.read_file(wfs_request_url)
    return data


def features_as_items(features: gpd.GeoDataFrame) -> Generator[pystac.Item, None, None]:
    features = features.to_crs(epsg=4326)
    crs_transformer = Transformer(2180, 4326)
    for _, feature in features.iterrows():
        # prepare bbox reprojected to EPSG:4326
        xmin, ymin = str(feature["lowerCorner"]).split(" ")
        left, bottom = crs_transformer.transform(float(xmin), float(ymin))
        xmax, ymax = str(feature["upperCorner"]).split(" ")
        right, top = crs_transformer.transform(float(xmax), float(ymax))
        bbox = (left, bottom, right, top)

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
