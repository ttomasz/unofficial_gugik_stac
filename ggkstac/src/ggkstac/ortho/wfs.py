import logging
from collections.abc import Generator

import requests.exceptions
from owslib.wfs import WebFeatureService, wfs200

from ggkstac.ortho.const import WFS_BASE_URL

logger = logging.getLogger(__name__)
_service: wfs200.WebFeatureService_2_0_0 | None = None


def wfs_service(max_retries: int = 5) -> wfs200.WebFeatureService_2_0_0:
    global _service
    if _service is not None:
        return _service
    
    def _get_service(try_number: int = 0) -> wfs200.WebFeatureService_2_0_0:
        try_number += 1
        try:
            logger.info("Connecting to WFS service: %s", WFS_BASE_URL)
            return WebFeatureService(url=WFS_BASE_URL, version="2.0.0") # type: ignore
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


def get_wfs_layer_ids() -> list[str]:
    return [layer.id for layer in wfs_layers_interator()]
