"""A small client for the LLM services that the admin app calls."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import httpx

from llm_service.admin_app.registry import service_url, valid_name
from llm_service.gateway.identity import ACTING_USER_HEADER


RETRY_AFTER = "Retry-After"


def retry_after_headers(headers: httpx.Headers) -> dict[str, str]:
    """Return the `Retry-After` header of a service answer when it is a whole number of 1 to 5 digits.

    Return an empty dict for each other value. The app passes no other header.
    """
    value = headers.get(RETRY_AFTER, "")
    if 1 <= len(value) <= 5 and value.isascii() and value.isdigit():
        return {RETRY_AFTER: value}
    return {}


class ServiceUnreachable(Exception):
    """The service did not answer, or it answered with a body that is not JSON.

    `sent` is True when the service got the request but did not answer in
    time. The action of the request can still finish then.
    """

    def __init__(self, message: str, sent: bool = False) -> None:
        super().__init__(message)
        self.sent = sent


class ServiceClient:
    """Call a service by name for a viewer and return the status and the JSON body."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def request(
        self,
        service: str,
        method: str,
        path: str,
        viewer: str | None,
        *,
        params: dict | None = None,
        json: object | None = None,
        timeout: float,
    ) -> tuple[int, object]:
        """Send one request and return the status and the JSON body. See `request_with_headers`."""
        status, body, _ = await self.request_with_headers(
            service, method, path, viewer, params=params, json=json, timeout=timeout
        )
        return status, body

    async def request_with_headers(
        self,
        service: str,
        method: str,
        path: str,
        viewer: str | None,
        *,
        params: dict | None = None,
        json: object | None = None,
        timeout: float,
    ) -> tuple[int, object, dict[str, str]]:
        """Send one request to the service `service`.

        Return the status, the JSON body and the headers that the app passes on.
        Today that is only a valid `Retry-After`. `viewer` goes into the acting user header. With None, the request has no
        acting user. Params with the value None are dropped. `timeout` is in
        seconds. Raise `ValueError` for a bad service name. Raise
        `ServiceUnreachable` for a transport error and for a body that is not JSON.
        """
        if not valid_name(service):
            raise ValueError("The service name is not valid.")
        kept = {name: value for name, value in (params or {}).items() if value is not None}
        # The auth proxy of the platform signs only the first request of a
        # connection. A new connection for each request keeps every call signed.
        headers = {"Connection": "close"}
        if viewer is not None:
            headers[ACTING_USER_HEADER] = viewer
        try:
            response = await self._http.request(
                method,
                service_url(service) + path,
                headers=headers,
                params=kept,
                json=json,
                timeout=timeout,
            )
            return response.status_code, response.json(), retry_after_headers(response.headers)
        except (httpx.ReadTimeout, httpx.WriteTimeout) as error:
            raise ServiceUnreachable(str(error), sent=True) from error
        except (httpx.HTTPError, ValueError) as error:
            raise ServiceUnreachable(str(error)) from error

    @contextlib.asynccontextmanager
    async def stream(
        self,
        service: str,
        path: str,
        viewer: str | None,
        *,
        json: object | None = None,
        timeout: float,
    ) -> AsyncIterator[httpx.Response]:
        """Send a POST to the service `service` and give the response before the app reads its body.

        The response closes when the block ends, whatever the cause. `viewer`
        and `timeout` work as in `request`. Raise `ValueError` for a bad service
        name. Raise `ServiceUnreachable` for a transport error before the response starts.
        """
        if not valid_name(service):
            raise ValueError("The service name is not valid.")
        # The app passes the raw bytes of the stream on, so the service must not compress them.
        headers = {"Connection": "close", "Accept-Encoding": "identity"}
        if viewer is not None:
            headers[ACTING_USER_HEADER] = viewer
        async with contextlib.AsyncExitStack() as stack:
            try:
                response = await stack.enter_async_context(
                    self._http.stream("POST", service_url(service) + path, headers=headers, json=json, timeout=timeout)
                )
            except (httpx.ReadTimeout, httpx.WriteTimeout) as error:
                raise ServiceUnreachable(str(error), sent=True) from error
            except httpx.HTTPError as error:
                raise ServiceUnreachable(str(error)) from error
            yield response
