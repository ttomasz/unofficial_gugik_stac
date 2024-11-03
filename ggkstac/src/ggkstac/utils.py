import logging
from datetime import timedelta

import httpx

logger = logging.getLogger(__name__)

async def download(url: str, params: dict | None, file_path: str) -> None:
    logger.debug("Downloading from URL: %s with params %s to file: %s", url, params, file_path)
    max_tries = 20  # because the servers suck and return random errors
    async def _dl(try_number: int = 1) -> int:
        size_in_bytes = 0
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
                        size_in_bytes += len(chunk)
                        f.write(chunk)
            return size_in_bytes
        except (httpx.ReadError, httpx.StreamError, httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
            if try_number >= max_tries:
                raise Exception(f"Could not download file: {file_path} from {url} with params {params}") from e
            else:
                logger.warning("Error during download %s. Retrying (%s/%s).", e, try_number + 1, max_tries)
                return await _dl(try_number=try_number + 1)
    total_size_in_bytes = await _dl()
    logger.debug("Finished download for file: %s (%s bytes)", file_path, total_size_in_bytes)
