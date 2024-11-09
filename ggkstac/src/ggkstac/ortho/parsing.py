import logging
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

import geopandas as gpd

from ggkstac import const
from ggkstac.ortho import const as ortho_const

logger = logging.getLogger(__name__)


def calculate_extent(gdf: gpd.GeoDataFrame) -> dict:
    xmin, ymin, xmax, ymax = gdf.total_bounds
    min_ts = ortho_const.tz.localize(datetime.fromisoformat(gdf["timePosition"].min())) # type: ignore
    max_ts = ortho_const.tz.localize(datetime.fromisoformat(gdf["timePosition"].max()) + timedelta(days=1, microseconds=-1)) # type: ignore
    return dict(
        spatial=dict(bbox=[[float(xmin), float(ymin), float(xmax), float(ymax)]]),
        temporal=dict(interval=[min_ts.astimezone(tz=UTC).isoformat(), max_ts.astimezone(tz=UTC).isoformat()])
    )


def features_as_items(features: gpd.GeoDataFrame, collection_id: str) -> Generator[dict, None, None]:
    for _, feature in features.iterrows():
        ymin, xmin = str(feature["lowerCorner"]).split(" ")
        ymax, xmax = str(feature["upperCorner"]).split(" ")
        bbox = [float(xmin), float(ymin), float(xmax), float(ymax)]

        item = dict(
            stac_version=const.STAC_VERSION,
            stac_extensions=const.STAC_EXTENSIONS,
            type="Feature",
            id=feature["gml_id"],
            bbox=bbox,
            geometry=feature["geometry"].__geo_interface__,
            properties={
                "datetime": (
                    ortho_const.tz.localize(datetime.fromisoformat(feature["timePosition"]))
                    .astimezone(tz=UTC)
                    .isoformat()
                ),
                "title": f"""{feature["zrodlo_danych"]}: {feature["godlo"]} - {feature["timePosition"]} - {feature["kolor"]}""",
                "description": (
f"""{feature["zrodlo_danych"]}
- rodzaj zdjęcia: {ortho_const.image_type_mapping.get(feature["kolor"])} ({feature["kolor"]})
- data zdjęcia: {feature["timePosition"]}
- data przyjęcia do zasobu: {feature["dt_pzgik|timePosition"]}
- id obszaru: {feature["godlo"]}
- czy arkusz w pełni wypełniony: {feature["czy_ark_wypelniony"]}
- numer zgłoszenia: {feature["nr_zglosz"]}
- moduł archiwizacji: {feature["modul_archiwizacji"]}
"""),
                "gsd": feature["piksel"],
                "proj:code": ortho_const.crs_names_mapping.get(feature["uklad_xy"]),
                "file:size": feature["wlk_pliku_MB"] * 1024 * 1024,
            },
            assets={
                "image": dict(
                    href=feature["url_do_pobrania"],
                    media_type=const.MEDIA_TYPE_GEOTIFF,
                    roles=["data"],
                ),
            },
            collection=collection_id,
            links=[
                dict(
                    rel="root",
                    href="../../catalog.json",
                    type=const.MEDIA_TYPE_JSON,
                ),
                dict(
                    rel="parent",
                    href="./collection.json",
                    type=const.MEDIA_TYPE_JSON,
                ),
            ],
        )
        yield item


def get_main_collection() -> dict:
    return dict(
        stac_version=const.STAC_VERSION,
        stac_extensions=const.STAC_EXTENSIONS,
        type="Collection",
        id=const.ID_COLLECTION_ORTHO,
        title="Ortofotomapy",
        description="Kolekcja z arkuszami ortofotomap, które można pobrać. Podzielona latami.",
        extent=dict(
            spatial=dict(bbox=[const.BBOX_POLAND]),
            temporal=dict(interval=[]),
        ),
        license=const.CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
        links=[
            dict(
                rel="root",
                href="../catalog.json",
                type=const.MEDIA_TYPE_JSON,
            ),
        ],
    )


def geoparquet_to_collection(
    gdf: gpd.GeoDataFrame,
) -> tuple[dict, list[dict]]:
    extent = calculate_extent(gdf=gdf)
    year_values = gdf["akt_rok"].unique() # type: ignore
    if len(year_values) == 1:
        collection_year = str(year_values[0])
        collection_id = const.ID_SUBCOLLECTION_ORTHO_TEMPLATE.format(year=collection_year)
        description = f"Arkusze ortofotomapy z roku: {collection_year}"
    elif len(year_values) > 1:
        year_min = year_values.min()
        year_max = year_values.max()
        collection_year = f"{year_min}-{year_max}"
        collection_id = const.ID_SUBCOLLECTION_ORTHO_TEMPLATE.format(year=collection_year)
        description = f"Arkusze ortofotomapy z lat: {collection_year}"
    else:
        raise Exception("Could not find year in layer features in akt_rok column.")
    logger.info("Preparing items for collection: %s", collection_id)
    items = list(
        features_as_items(
            features=gdf,
            collection_id=collection_id,
        )
    )
    logger.info("Items for collection: %s are ready. Adding to collection object.", collection_id)
    subcollection = dict(
        stac_version=const.STAC_VERSION,
        stac_extensions=const.STAC_EXTENSIONS,
        type="Collection",
        id=collection_id,
        title=collection_year,
        description=description,
        extent=extent,
        license=const.CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
        links=[
            # /catalog.json
            # /ortho/collection.json
            # /ortho/1957/collection.json
            # /ortho/1957/item1.json
            # /ortho/1957/item2.json
            dict(
                rel="root",
                href="../../catalog.json",
                type=const.MEDIA_TYPE_JSON,
            ),
            dict(
                rel="parent",
                href="../collection.json",
                type=const.MEDIA_TYPE_JSON,
            ),
            *(dict(rel="item", href=f"./{i['id']}.json", title=i["properties"]["title"], type=const.MEDIA_TYPE_GEOJSON) for i in items),
        ],
    )
    logger.info("Collection: %s is ready.", collection_id)
    return subcollection, items
