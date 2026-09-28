# Changelog

## 1.1.0

### Added

- Nested JSON, JSONL and Parquet data is taken apart into tables. A nested
  object becomes columns joined with `__`. A list of objects becomes a child
  stream, with `_parent__` columns, `_parent_row`, `_index` and `_row_key`.
  Other lists become JSON text columns. See "Nested data" in the README.
- A sync reads each object once, and routes rows to every selected stream.

### Changed

- A nested object is no longer one `object` column, and a list is no longer
  one `array` column. A catalog saved on version 1.0.0 for nested data
  still syncs: its `object` columns and its `array` columns for lists of
  objects stay null, and its other `array` columns keep their values. Run
  discovery again and save the catalog to get the new columns and child
  streams.
- A Parquet map column is JSON text instead of an `object` column.
- Files with no nested objects or lists give the same catalog and records
  as version 1.0.0.

## 1.0.0

- First release.
