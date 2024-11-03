import argparse
import asyncio
import logging
from datetime import timedelta
from pathlib import Path
from time import perf_counter

import pystac

from .catalog import get_main_catalog
from .log import logger, set_logging_level
from .ortho.download import download_and_save_to_geoparquet
from .ortho.parsing import geoparquet_to_collection
from .ortho.wfs import get_wfs_layer_ids


def get_parser() -> argparse.ArgumentParser:
    async def download_all(output_folder: str):
        paths = await download_and_save_to_geoparquet(layer_ids=get_wfs_layer_ids(), output_directory=output_folder)
        logger.info("Paths of saved files: %s", paths)

    def convert_geoparquet(input_folder: str, output_folder: str):
        input_dir = Path(input_folder)
        if not (input_dir.exists() and input_dir.is_dir()):
            raise Exception(f"Given input path: {input_folder} does not exist or is not a directory.")
        output_dir = Path(output_folder)
        if not output_dir.exists():
            logger.warning("Directory %s does not exist. Creating.", output_dir.absolute().resolve())
            output_dir.mkdir(parents=True)
        ortho_collection = geoparquet_to_collection(folder=input_dir)
        catalog = get_main_catalog()
        catalog.add_child(child=ortho_collection)
        logger.info("Saving catalog to: %s", output_dir)
        catalog.normalize_and_save(output_dir.as_posix(), pystac.CatalogType.SELF_CONTAINED)
        logger.info("Finished saving catalog.")

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
    subparser_convert.set_defaults(func=convert_geoparquet, action="convert_geoparquet")

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
