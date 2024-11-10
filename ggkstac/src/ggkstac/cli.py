import argparse
import asyncio
import logging
import shutil
from datetime import timedelta
from pathlib import Path
from time import perf_counter

import geopandas as gpd
import orjson

from . import const
from .catalog import get_main_catalog
from .log import logger, set_logging_level
from .ortho.download import download_and_save_to_geoparquet
from .ortho.parsing import geoparquet_to_collection, get_main_collection
from .ortho.wfs import get_wfs_layer_ids


def get_parser() -> argparse.ArgumentParser:
    async def download_all(output_folder: str):
        paths = await download_and_save_to_geoparquet(layer_ids=get_wfs_layer_ids(), output_directory=output_folder)
        logger.info("Paths of saved files: %s", paths)

    def convert_geoparquet_files(input_folder: str, output_folder: str):
        input_dir = Path(input_folder)
        if not (input_dir.exists() and input_dir.is_dir()):
            raise Exception(f"Given input path: {input_folder} does not exist or is not a directory.")
        geoparquet_files = list(input_dir.glob("*.parquet"))
        if len(geoparquet_files) == 0:
            raise Exception(f"Did not find any Parquet files in {input_folder}")
        output_dir = Path(output_folder)
        if output_dir.exists():
            if next(output_dir.iterdir(), None):
                logger.warning("Directory %s is not empty. Removing contents...", output_dir)
                shutil.rmtree(output_dir)
                output_dir.mkdir()
        else:
            logger.warning("Directory %s does not exist. Creating.", output_dir.absolute().resolve())
            output_dir.mkdir(parents=True)
        catalog_file = output_dir / "catalog.json"
        ortho_collection_dir = output_dir / const.ID_COLLECTION_ORTHO
        ortho_collection_file = output_dir / const.ID_COLLECTION_ORTHO / "collection.json"
        ortho_collection_dir.mkdir()
        catalog = get_main_catalog()
        ortho_collection = get_main_collection()
        catalog["links"].append(dict(rel="child", href=f"./{const.ID_COLLECTION_ORTHO}/collection.json", type=const.MEDIA_TYPE_JSON, title=ortho_collection["title"]))
        for file in input_dir.glob("*.parquet"):
            logger.info("Processing file: %s", file)
            gdf = gpd.read_parquet(file)
            sub_collection, items = geoparquet_to_collection(gdf=gdf)
            ortho_collection["links"].append(dict(rel="child", href=f"./{sub_collection['id']}/collection.json", type=const.MEDIA_TYPE_JSON, title=sub_collection["title"]))
            ortho_collection["extent"]["temporal"]["interval"].append(sub_collection["extent"]["temporal"]["interval"][0])
            sub_collection_dir: Path = output_dir / const.ID_COLLECTION_ORTHO / sub_collection["id"]
            sub_collection_file = sub_collection_dir / "collection.json"
            sub_collection_dir.mkdir()
            logger.info("Writing subcollection file")
            with sub_collection_file.open("wb") as f:
                f.write(orjson.dumps(sub_collection))
            logger.info("Writing item files")
            for item in items:
                item_file = sub_collection_dir / f"{item['id']}.json"
                with item_file.open("wb") as f:
                    f.write(orjson.dumps(item))
            logger.info("Done writing item files")
        interval_values = [elem for interval in ortho_collection["extent"]["temporal"]["interval"] for elem in interval]
        covering_interval = [min(interval_values), max(interval_values)]
        ortho_collection["extent"]["temporal"]["interval"].insert(0, covering_interval)
        with ortho_collection_file.open("wb") as f:
            f.write(orjson.dumps(ortho_collection))
        with catalog_file.open("wb") as f:
            f.write(orjson.dumps(catalog))


    parser = argparse.ArgumentParser(
        prog="ggkstac",
        description="Utility to create STAC catalog presenting open data shared by Chief Surveyor of Poland.",
        allow_abbrev=False,
    )
    parser.add_argument("--log-level", type=str, choices=logging.getLevelNamesMapping().keys(), default="INFO")
    subparsers = parser.add_subparsers(required=True)

    subparser_download = subparsers.add_parser(
        name="download",
        description="Parameters for 'download' action.",
        help="Download Features from WFS layer and save as GeoParquet files.",
        allow_abbrev=False,
    )
    subparser_download.add_argument("--layer-ids", nargs="+", required=True, help="Ids of WFS layers to download.")
    subparser_download.add_argument("--output-folder", type=str, required=True, help="Folder where WFS features will be saved as GeoParquet files.")
    subparser_download.set_defaults(func=download_and_save_to_geoparquet, action="download")

    subparser_download_all = subparsers.add_parser(
        name="download_all",
        description="Parameters for 'download_all' action.",
        help="Download Features from all WFS layers and save as GeoParquet files.",
        allow_abbrev=False,
    )
    subparser_download_all.add_argument("--output-folder", type=str, required=True, help="Folder where WFS features will be saved as GeoParquet files.")
    subparser_download_all.set_defaults(func=download_all, action="download_all")

    subparser_list = subparsers.add_parser(
        name="layer_ids",
        description="There are no parameters for 'layer_ids' action.",
        help="Lists Layer IDs from WFS.",
        allow_abbrev=False,
    )
    subparser_list.set_defaults(func=lambda: print(get_wfs_layer_ids()), action="layer_ids")

    subparser_convert = subparsers.add_parser(
        name="convert_geoparquet",
        description="Parameters for 'convert_geoparquet' action.",
        help="Comverts saved GeoParquet files into STAC Catalog.",
        allow_abbrev=False,
    )
    subparser_convert.add_argument("--input-folder", type=str, required=True, help="Folder where saved GeoParquet files are.")
    subparser_convert.add_argument("--output-folder", type=str, required=True, help="Folder where STAC Catalog will be saved.")
    subparser_convert.set_defaults(func=convert_geoparquet_files, action="convert_geoparquet")

    return parser


def main() -> None:
    parser = get_parser()
    args = parser.parse_args()
    set_logging_level(args.log_level)
    logger.debug("Parsed args: %s", args)
    parameters = args.__dict__.copy()
    parameters.pop("log_level")
    func = parameters.pop("func")
    action = parameters.pop("action")

    logger.info("Begin running action: %s", action)
    start_time = perf_counter()
    if asyncio.coroutines.iscoroutinefunction(func):
        logger.debug("Detected async function. Running Coroutine with asyncio.")
        asyncio.run(func(**parameters))
    else:
        logger.debug("Detected regular function. Running.")
        func(**parameters)
    end_time = perf_counter()
    time_took = timedelta(seconds=end_time - start_time)
    logger.info("Finished running action: %s in %s.", action, time_took)
