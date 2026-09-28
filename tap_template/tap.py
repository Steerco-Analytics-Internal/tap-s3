"""Template tap class. Rename to TapYourThing and customize."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_template.streams import ExampleStream

STREAM_TYPES = [ExampleStream]


class TapTemplate(Tap):
    """Singer tap template — rename to TapYourThing."""

    name = "tap-template"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="API key for the upstream service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        return [stream_type(tap=self) for stream_type in STREAM_TYPES]


if __name__ == "__main__":
    TapTemplate.cli()
