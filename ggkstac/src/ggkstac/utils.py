import logging
from datetime import timedelta

import httpx

logger = logging.getLogger(__name__)

async def download(url: str, params: dict, file_path: str) -> None:
    logger.info("Downloading from URL: %s with params %s to file: %s", url, params, file_path)
    max_tries = 20  # because the servers suck and return random errors
    async def _dl(try_number: int = 1) -> None:
        try:
            async with (
                httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=10),
                    timeout=timedelta(minutes=10).total_seconds(),
                    follow_redirects=True,
                ) as client,
                client.stream(method="GET", url=url, params=params) as response
            ):
                logger.debug("Response status code: %s", response.status_code)
                response.raise_for_status()
                logger.debug("Formatted URL: %s", response.url)
                with open(file_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8_000):
                        f.write(chunk)
        except (httpx.ReadError, httpx.StreamError, httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
            if try_number >= max_tries:
                raise Exception(f"Could not download file: {file_path} from {url} with params {params}") from e
            else:
                logger.warning("Error during download %s. Retrying (%s/%s).", e, try_number + 1, max_tries)
                await _dl(try_number=try_number + 1)
    await _dl()
    logger.info("Finished download for file: %s", file_path)
