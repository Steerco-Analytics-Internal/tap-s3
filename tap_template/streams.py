"""Stream definitions. Add one class per resource you want to sync."""

from singer_sdk import typing as th

from tap_template.client import TemplateStream


class ExampleStream(TemplateStream):
    name = "example"
    path = "/example"
    primary_keys = ["id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()
