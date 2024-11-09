import logging

from .const import ID_CATALOG, MEDIA_TYPE_JSON, STAC_EXTENSIONS, STAC_VERSION

logger = logging.getLogger(__name__)


def get_main_catalog() -> dict:
    catalog = dict(
        id=ID_CATALOG,
        title="Katalog otwartych danych GUGiK",
        description="Katalog STAC pozwalający przeglądać dane udostępniane przez Główny Urząd Geodezji i Kartografii.",
        stac_version=STAC_VERSION,
        stac_extensions=STAC_EXTENSIONS,
        type="Catalog",
        extra_fields={
            "language": {
                "code": "pl",
                "name": "Polski",
                "alternate": "Polish",
                "dir": "ltr",
            },
        },
        links=[
            dict(
                rel="root",
                href="./catalog.json",
                type=MEDIA_TYPE_JSON,
            )
        ],
    )
    return catalog
