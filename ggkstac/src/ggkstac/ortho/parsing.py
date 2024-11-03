import logging
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pystac
import pystac.media_type

from ggkstac import const
from ggkstac.ortho import const as ortho_const

logger = logging.getLogger(__name__)


def geoparquet_iterator(folder: Path) -> Generator[tuple[Path, gpd.GeoDataFrame], None, None]:
    for item in folder.iterdir():
        if item.is_file() and item.suffix == ".parquet":
            yield item, gpd.read_parquet(item)


def calculate_extent(gdf: gpd.GeoDataFrame) -> pystac.Extent:
    xmin, ymin, xmax, ymax = gdf.total_bounds
    min_ts = datetime.fromisoformat(gdf["timePosition"].min()) # type: ignore
    max_ts = datetime.fromisoformat(gdf["timePosition"].max()) # type: ignore
    return pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[[xmin, ymin, xmax, ymax]]),
        temporal=pystac.TemporalExtent(intervals=[min_ts, max_ts])
    )


def features_as_items(features: gpd.GeoDataFrame) -> Generator[pystac.Item, None, None]:
    for _, feature in features.iterrows():
        ymin, xmin = str(feature["lowerCorner"]).split(" ")
        ymax, xmax = str(feature["upperCorner"]).split(" ")
        bbox = [float(xmin), float(ymin), float(xmax), float(ymax)]

        item = pystac.Item(
            id=feature["gml_id"],
            geometry=feature["geometry"].__geo_interface__,
            bbox=bbox,
            datetime=datetime.fromisoformat(feature["timePosition"]).replace(tzinfo=ortho_const.tz),
            properties={
                "title": f"""{feature["zrodlo_danych"]}: {feature["godlo"]} - {feature["timePosition"]} - {feature["kolor"]}""",
                "description": f"""
{feature["zrodlo_danych"]}
- rodzaj zdjęcia: {ortho_const.image_type_mapping.get(feature["kolor"])} ({feature["kolor"]})
- data zdjęcia: {feature["timePosition"]}
- data przyjęcia do zasobu: {feature["dt_pzgik|timePosition"]}
- id obszaru: {feature["godlo"]}
- czy arkusz w pełni wypełniony: {feature["czy_ark_wypelniony"]}
- numer zgłoszenia: {feature["nr_zglosz"]}
- moduł archiwizacji: {feature["modul_archiwizacji"]}
""".strip(),
                "gsd": feature["piksel"],
                "proj:code": ortho_const.crs_names_mapping.get(feature["uklad_xy"]),
                "file:size": feature["wlk_pliku_MB"] * 1024 * 1024,
            },
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


def get_main_collection(extent: pystac.Extent) -> pystac.Collection:
    logger.info("Creating main Orthophotomap collection.")
    return pystac.Collection(
        id=const.ID_COLLECTION_ORTHO,
        title="Ortofotomapy",
        description="Kolekcja z arkuszami ortofotomap, które można pobrać. Podzielona latami.",
        extent=extent,
        license=const.CC0,
        keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
    )


def geoparquet_to_collection(folder: Path) -> pystac.Collection:
    subcollections: list[pystac.Collection] = []
    extents: list[pystac.Extent] = []
    logger.info("Preparing Ortho collection from geoParquet files in folder: %s", folder)
    for file, gdf in geoparquet_iterator(folder=folder):
        logger.info("Processing file: %s", file)
        extent = calculate_extent(gdf=gdf)
        extents.append(extent)
        items = features_as_items(features=gdf)
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
            raise Exception(f"Could not find year in layer features in akt_rok column: {file.stem}")
        collection = pystac.Collection(
            id=collection_id,
            title=collection_year,
            description=description,
            extent=extent,
            license=const.CC0,
            keywords=["ortofotomapa", "ortofoto", "zdjęcia lotnicze"],
        )
        collection.add_items(items=items)
        subcollections.append(collection)
        logger.info("Collection id: %s is ready.", collection_id)
    ortho_collection = get_main_collection(
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(const.BBOX_POLAND),
            temporal=pystac.TemporalExtent(intervals=[interval for e in extents for interval in e.temporal.intervals ]) # type: ignore
        ),
    )
    logger.info("Adding children to the main Ortho collection.")
    ortho_collection.add_children(children=subcollections)
    logger.info("Finished preparing main Ortho collection.")
    return ortho_collection
