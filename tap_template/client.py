"""REST client base. Replace with the client your upstream service needs."""

from typing import Any, Dict, Iterable

from singer_sdk.streams import RESTStream


class TemplateStream(RESTStream):
    """Base class for streams hitting the upstream API."""

    @property
    def url_base(self) -> str:
        return "https://api.example.com/v1"

    @property
    def http_headers(self) -> Dict[str, str]:
        api_key = self.config["api_key"]
        return {"Authorization": f"Bearer {api_key}"}

    def parse_response(self, response: Any) -> Iterable[Dict[str, Any]]:
        yield from response.json().get("data", [])
