# Changelog

## 1.1.1

### Fixed

- The tap reads `_hg_max_records_limit`, the per-stream record limit that
  Hotglue's field-sample job sends. Before, the tap ignored it, so a
  field-sample job read every object in full.
- A top-level stream that reaches its record limit ends the sync without an
  error. Before, the SDK raised an abort exception at the limit.
- A limited sync stops reading objects once every selected stream in the
  group has its rows.

## 1.1.0

### Added

- Nested JSON, JSONL and Parquet data is taken apart into tables. A nested
  object becomes columns joined with `__`. A list of objects becomes a child
  stream, with `_parent__` columns, `_parent_row`, `_index` and `_row_key`.
  Other lists become JSON text columns. See "Nested data" in the README.
- A sync reads each object once, and routes rows to every selected stream.
- Column names are one-to-one with paths. Keys are encoded with `_xHH_`
  escapes where needed, so two paths never share a column.
- Each child stream's catalog entry records its parent in the metadata keys
  `tap-s3.parent-stream` and `tap-s3.list-path`. When a catalog store drops
  those keys, the tap links a child stream by an exact match of its name
  against the streams it names in the bucket. It plans only the file
  streams it needs, and skips a child whose name, or a sibling's name,
  carries a clash suffix, or whose columns don't match.

### Changed

- A nested object is no longer one `object` column, and a list is no longer
  one `array` column. A catalog saved on version 1.0.0 for nested data needs
  discovery again. Run discovery and save the catalog.
- A column named like a metadata column is encoded, as in `_x5f_s3_key`,
  instead of renamed to `_s3_key_source`. This applies to CSV headers and
  top-level JSON keys.
- A CSV header or top-level JSON key that reads as an encoded name, such as
  `a__b`, is encoded, as in `a_x5f__b`. A CSV header and a JSON key with the
  same name now give the same column.
- A stream with a record limit, such as in a field-sample job, writes no
  bookmark changes.
- A Parquet map column is JSON text instead of an `object` column.
- Files with no nested objects or lists give the same catalog and records
  as version 1.0.0, except for the two renames above: columns named like
  metadata columns, and columns that read as encoded names, such as `a__b`.

## 1.0.0

- First release.
