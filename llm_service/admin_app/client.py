"""A small client for the admin endpoints of the LLM service."""

from __future__ import annotations

import httpx

from llm_service.gateway.identity import ACTING_USER_HEADER


class ServiceUnreachable(Exception):
    """The service did not answer, or it answered with a body that is not JSON.

    `sent` is True when the service got the request but did not answer in
    time. The action of the request may still finish then.
    """

    def __init__(self, message: str, sent: bool = False) -> None:
        super().__init__(message)
        self.sent = sent


class ServiceClient:
    """Call the service for a viewer and return the status and the JSON body."""

    def __init__(self, base_url: str, http: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http

    async def request(
        self,
        method: str,
        path: str,
        viewer: str,
        *,
        params: dict | None = None,
        json: object | None = None,
        timeout: float,
    ) -> tuple[int, object]:
        """Send one request with the viewer name in the acting user header.

        Params with the value None are dropped. `timeout` is in seconds. Raise
        `ServiceUnreachable` for a transport error and for a body that is not JSON.
        """
        kept = {name: value for name, value in (params or {}).items() if value is not None}
        try:
            response = await self._http.request(
                method,
                self._base_url + path,
                headers={ACTING_USER_HEADER: viewer},
                params=kept,
                json=json,
                timeout=timeout,
            )
            return response.status_code, response.json()
        except (httpx.ReadTimeout, httpx.WriteTimeout) as error:
            raise ServiceUnreachable(str(error), sent=True) from error
        except (httpx.HTTPError, ValueError) as error:
            raise ServiceUnreachable(str(error)) from error
