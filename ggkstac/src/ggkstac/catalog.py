import logging

import pystac

from .const import ID_CATALOG
from .orto import build_ortho_collection

logger = logging.getLogger(__name__)

async def build_catalog() -> pystac.Catalog:
    logger.info("Building main Catalog...")
    catalog = pystac.Catalog(
        id=ID_CATALOG,
        title="Katalog otwartych danych GUGiK",
        description="Katalog STAC pozwalający przeglądać dane udostępniane przez Główny Urząd Geodezji i Kartografii.",
    )
    logger.info("Building and adding Ortho collection...")
    ortho_collection = await build_ortho_collection()
    catalog.add_child(child=ortho_collection)
    logger.info("Catalog is ready.")
    return catalog
