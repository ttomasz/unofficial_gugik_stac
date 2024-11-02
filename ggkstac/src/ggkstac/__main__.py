import asyncio

import pystac

from .catalog import build_catalog
from .log import logger

logger.info("Hello from ggkstac!")
catalog = asyncio.run(build_catalog())
catalog.normalize_and_save("./dist_stac/", pystac.CatalogType.SELF_CONTAINED)
