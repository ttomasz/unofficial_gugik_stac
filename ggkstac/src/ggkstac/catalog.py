import logging

import pystac

from .const import ID_CATALOG

logger = logging.getLogger(__name__)


def get_main_catalog() -> pystac.Catalog:
    catalog = pystac.Catalog(
        id=ID_CATALOG,
        title="Katalog otwartych danych GUGiK",
        description="Katalog STAC pozwalający przeglądać dane udostępniane przez Główny Urząd Geodezji i Kartografii.",
        stac_extensions=[
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
            "https://stac-extensions.github.io/language/v1.0.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        ],
        extra_fields={
            "language": {
                "code": "pl",
                "name": "Polski",
                "alternate": "Polish",
                "dir": "ltr",
            },
        },
    )
    return catalog
