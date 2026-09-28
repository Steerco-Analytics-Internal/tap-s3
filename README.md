# tap-s3

`tap-s3` is a Singer tap that reads tabular files from an AWS S3 bucket. It is
built with the [Meltano Singer SDK](https://sdk.meltano.com).

Hotglue runs it as a custom connector. Its discovery fills the Hotglue
catalog, which Steerco's field-mapping UI and field-sample jobs read. The
Hotglue S3 connector copies raw files and has no discovery, so this tap
exists to fill that gap.

## Configuration

The first five settings match the Hotglue S3 connector form. Keep their names,
so the connection form stays the same.

| Setting | Required | Description |
|---|---|---|
| `aws_access_key_id` | Yes | The access key ID of an IAM user that can read the bucket. |
| `aws_secret_access_key` | Yes | The secret access key. The config schema marks it as secret. |
| `bucket` | Yes | The bucket name. |
| `path_prefix` | No | The folder to read. Leave it empty to read from the bucket root. |
| `incremental_mode` | No | Defaults to `true`. When `false`, every sync reads every object. |
| `region` | No | The bucket's region. When empty, the tap calls `GetBucketLocation`. |
| `start_date` | No | The tap ignores objects last modified before this date and time. |
| `lookback_minutes` | No | Defaults to 60. How far back each sync checks again for late objects. |
| `exclude_pattern` | No | A regular expression. The tap ignores objects whose key matches it. |

`incremental_mode` accepts a boolean, a string, or the number 0 or 1, because
Hotglue can send any of them. The strings `true`, `yes` and `1` mean true, and `false`, `no` and
`0` mean false. Case and surrounding spaces don't matter. A missing value or
an empty string means `true`. Any other value is a config error.

`exclude_pattern` is matched with a regex search against the key relative to
`path_prefix`. Discovery and sync both apply it. Use it to leave out files
that aren't data, such as manifests. For example, `(^|/)manifest\.json$`
excludes every `manifest.json`.

The IAM user needs `s3:ListBucket` and `s3:GetObject`. Without `region`, it
also needs `s3:GetBucketLocation`. If that call is denied, the tap reads the
region from the `x-amz-bucket-region` header of the error. If the header is
missing, the tap stops and asks you to set `region`.

The tap treats `path_prefix` as a folder. `exports`, `exports/` and
`/exports/` all read `exports/`. None of them read `exports2/`.

## Streams

The tap lists every object under the prefix, across all result pages. It
names each stream from the object's key, relative to the prefix.

- An object inside a folder belongs to the stream named after its top-level
  folder. Depth does not matter: `contacts/2026/09/part-1.csv` is in stream
  `contacts`.
- An object at the prefix root belongs to the stream named after its file
  stem. The tap removes trailing date, timestamp and numeric tokens from the
  stem. `accounts_2026-09-01.csv`, `accounts-20260901T1200.csv`,
  `accounts (1).csv` and `accounts.csv` are all stream `accounts`.
- A token only counts when a separator comes before it: `-`, `_`, `.` or a
  space. So `sales_q3.csv` is stream `sales_q3`, and `report2026.csv` is
  stream `report2026`.
- A stem with no letters keeps its tokens. `2026-09-01.csv` is stream
  `2026_09_01`.
- The tap replaces each run of characters outside `[A-Za-z0-9_]` with `_`,
  and trims `_` from both ends. It keeps the case, so `Accounts.csv` and
  `accounts.csv` are two streams.
- A root file and a top-level folder with the same name form one stream. For
  example, `accounts.csv` and `accounts/2026.csv` are both in `accounts`.
- Different names can clean up to the same stream name, such as the folders
  `Sales Data` and `Sales-Data`. They form one stream, and the tap logs a
  warning that names both.

The tap skips these objects without a warning:

- Folder markers, which are keys that end in `/`.
- Zero-byte objects.
- Hidden objects. A key is hidden when any segment after the prefix starts
  with `.` or `_`, such as `_SUCCESS` or `contacts/_temporary/part.csv`.

The tap skips objects with an unsupported extension, and names with no
characters left after cleanup. It logs one warning that lists their keys,
capped at 100 keys.

## Formats

The tap picks the format from the extension. Each format also works with a
`.gz` suffix.

| Extension | Format |
|---|---|
| `.csv`, `.tsv`, `.txt` | Delimited text. The tap detects `,`, tab, `;` or `\|` from the first 16 KiB. |
| `.json` | An array of objects, or an object with one field that holds an array of objects. The tap streams it with ijson. |
| `.jsonl`, `.ndjson` | One JSON object per line. The tap skips blank lines. |
| `.parquet` | Parquet. The tap reads it by row group with ranged GET requests. |

For a `.json` object, only a field whose every item is an object counts. The
tap reads the whole structure once before it yields any row. So a file with
two such fields fails at discovery, even when the rows come first. A second
pass then streams the chosen field.

JSON, JSONL and Parquet data can be nested at any depth. See
[Nested data](#nested-data).

Delimited files:

- The first row is the header. A blank header becomes `column_N`. A repeated
  header gets a suffix, such as `name_2`.
- The tap removes a UTF-8 byte order mark from the raw bytes, then decodes
  the file as UTF-8. If the bytes aren't valid UTF-8, it reads the file again
  as cp1252, then as latin-1. Latin-1 maps every byte, so decoding can't fail.
- A row with extra cells puts them in `column_N`. A row with missing cells
  gets null for them. The tap skips blank lines.
- An empty cell is null.

The tap streams delimited, JSON and JSONL files. A gzipped Parquet file can't
seek, so the tap decompresses it to a temporary file first.

## Schema discovery

For each stream, the tap samples the 5 most recent objects and up to 1,000
rows from each. The columns are the union across those objects.

Every data column from a `.csv`, `.tsv` or `.txt` file is text, typed
`["string", "null"]`, with no inference. An empty cell is null. Steerco's
sync schema converts text to numbers, dates and booleans, so nothing is lost
downstream. A value outside the sample, such as `N/A` in a column of
numbers, can't break the sync.

JSON and JSONL columns keep their JSON types. The tap infers them
conservatively: a column is `integer`, `number`, `boolean` or `date-time`
only when every non-empty sampled value has that type. Otherwise it is
`string`.

- A JSON integer and a JSON number together make `number`.
- A JSON string can only become `date-time`, and only in ISO 8601, such as
  `2026-09-01` or `2026-09-01T12:30:00Z`. The JSON string `"42"` stays a
  string, and so does `"09/01/2026"`.
- Nested objects and lists are taken apart into columns and child streams.
  See [Nested data](#nested-data).
- When files of different formats share a stream, a column with different
  types across them becomes `string`. For example, a column that is text in a
  CSV file and an integer in a JSONL file is `string`.
- Parquet columns use the file's own types. A Parquet date becomes a string
  with format `date`.
- Discovery skips an object it can't parse. It logs the object's key and the
  error, and samples the next object instead.

Every data column is nullable. Every record also carries these columns:

| Column | Type | Description |
|---|---|---|
| `_s3_key` | string | The object key. |
| `_s3_last_modified` | date-time | The object's LastModified value, in UTC. |
| `_row_number` | integer | The row's position in the object, starting at 1. |

The primary key is `_s3_key` and `_row_number`. The replication key is
`_s3_last_modified`. A source column with one of these names gets its first
underscore encoded, as in `_x5f_s3_key`, and the tap logs a warning.

## Nested data

Version 1.1.0 takes nested JSON, JSONL and Parquet data apart into tables,
so a file can have any shape. Delimited files don't change. A file with no
nested objects or lists gives the same catalog and records as version 1.0.0.

### Nested objects become columns

The tap joins the path to each nested value with `__`. For example,
`valueRealization.grossSales` becomes the column
`valueRealization__grossSales`, and `licenseUtilization.storage.used`
becomes `licenseUtilization__storage__used`.

- Nesting stops at 10 levels, counting objects and lists. A deeper object or
  list stays whole, as JSON text in one column.
- A null or empty object adds no columns. Its fields are null in that row.

Parquet struct columns follow the same rules.

### Column names

Each column name stands for exactly one path, and the path alone decides the
name. So two paths never share a column, whatever keys a row has and in
whatever order.

- A key at the top level of a row keeps its own name, so flat files keep
  their columns.
- In a joined name, each key keeps its Unicode letters and digits. Any other
  character becomes `_xHH_`, with its code point in hex. For example,
  `x-y` becomes `x_x2d_y`, and a space becomes `_x20_`.
- An underscore stays when it sits between two kept characters. Other
  underscores become `_x5f_`, so a key never holds the `__` that joins a
  path. For example, the key `a__b` inside an object becomes `a_x5f__b`.
- A top-level key that reads as a joined name, such as `a__b`, is encoded
  the same way, so it can't take the column of the path `a.b`.
- A top-level key named like a metadata column, such as `_s3_key`, gets its
  first underscore encoded, as in `_x5f_s3_key`. The metadata column keeps
  its name. The tap logs one warning per stream for the keys it renames.

### Lists of objects become child streams

A list whose items are all objects becomes a child stream, with one row per
item. The child stream's name is the parent stream's name, then `__`, then
the list's path joined with `__`. For example, the list
`valueRealization.adjustments` in stream `customers_nested` becomes the
stream `customers_nested__valueRealization__adjustments`.

- The tap takes each item apart by the same rules. A list inside an item
  becomes a grandchild stream, and so on.
- An empty list adds no rows. A null item in a list is skipped, and the
  other items keep their positions.
- A Parquet list of structs follows the same rules.

Each child row carries these columns:

| Column | Type | Description |
|---|---|---|
| `_parent__<key>` | nullable | Each top-level plain value of the parent row, such as `_parent__domain`. A plain value is neither an object nor a list. |
| `_parent_row` | string | The parent row's key. For a top-level parent, that is `<_s3_key>#<_row_number>`. For a deeper parent, it is the parent's `_row_key`. |
| `_index` | integer | The item's position in the list, starting at 0. |
| `_row_key` | string | This row's key, such as `<parent key>/valueRealization__adjustments#0`. |
| `_s3_key` | string | The object key. |
| `_s3_last_modified` | date-time | The object's LastModified value, in UTC. |

The primary key of a child stream is `_s3_key` and `_row_key`. The
replication key is `_s3_last_modified`. An item's key named like one of these
columns, or starting with `_parent__`, gets its first underscore encoded, as
in `_x5f_index`.

### Stream names

A child stream's name joins its parent stream's name and the list's path with
`__`, and encodes the keys like column names. When that name is already taken,
for example by a file stream from `orders__items.json` next to the list
`items` in `orders.json`, the child stream gets a suffix, such as
`orders__items_2`, and the tap logs a warning. File streams keep their names.

Each child stream's catalog entry records its parent in metadata at breadcrumb
`[]`:

| Key | Value |
|---|---|
| `tap-s3.parent-stream` | The name of the parent stream. |
| `tap-s3.list-path` | The list's path inside a parent row, as a JSON array of keys. |

A sync links each child stream to its parent by these keys. A catalog store
might drop metadata keys it doesn't know. When a child stream's entry has no
`tap-s3.parent-stream` key, the tap names the streams in the bucket the way
discovery does, and links the child whose name matches exactly. The naming is
deterministic, so the same bucket gives the same names. The tap never guesses
a parent from part of a name. It logs, once per child stream, whether it
linked the child by its metadata or by its name.

A child stream that the tap can't link is skipped with a warning, and its
bookmark doesn't move. That happens when the parent is missing, or when the
bucket no longer has a child stream with that name. For example, say a file
`orders__items.json` appears after discovery, next to the list `items` in
`orders.json`. The file stream then takes the name `orders__items`, and the
list's child stream becomes `orders__items_2`. A catalog entry
`orders__items` without the metadata no longer names a child stream, so the
tap skips it rather than guess. With the metadata, the child stream still
syncs. To pick up such a change, run discovery again.

### Other lists become JSON text

A list of plain values, such as tags, becomes one text column that holds the
JSON, such as `["a", "b"]`. So does a list that mixes objects with other
values, and a list of lists. A Parquet list of plain values and a Parquet map
become JSON text too.

### Discovery and sync with child streams

- Discovery finds child streams in the same sample it takes for the parent.
  A child stream's schema is the union across the sampled rows and files.
- Child streams are ordinary streams in the catalog. Select them, or leave
  them out, like any other stream. A child stream can be selected while its
  parent is not.
- A sync reads each object once, and routes its rows to every selected
  stream. It holds one record at a time, never a whole file.
- Each stream keeps its own bookmark and window. An object counts as done for
  a stream only after the tap emits its rows for every selected stream. A
  child stream selected later reads the older objects for itself, and the
  other streams don't read them again.
- A child stream that appears after discovery is ignored until you run
  discovery again.
- A record limit, such as in a field-sample job, can drop some rows. So a
  stream with a record limit writes no bookmark changes.

### Change from version 1.0.0

Version 1.0.0 typed a nested object as an `object` column, and a list as an
`array` column. Version 1.1.0 takes them apart as described above. A catalog
saved on version 1.0.0 for nested data needs discovery again: run discovery
and save the catalog.

Version 1.0.0 renamed a column named like a metadata column to
`<name>_source`. Version 1.1.0 encodes it instead, as in `_x5f_s3_key`.

## Sync

During a sync, the tap uses the schemas in the catalog. It doesn't sample the
bucket again.

- The tap reads a stream's objects oldest first, by LastModified, then by key.
- The tap writes date-time values with a UTC offset. A value without a zone,
  including a Parquet timestamp, is taken as UTC.
- A value that JSON can't hold, such as NaN or infinity, becomes null. So
  does text that overflows to infinity, such as `1e400`. An empty string is
  null in every column that isn't a string.
- The tap drops a column that isn't in the catalog schema. It logs one warning
  per stream.
- The tap stops early when the SDK's record limit is reached. Hotglue
  field-sample jobs rely on this.

### Incremental reads

A stream reads incrementally when `incremental_mode` is true and the catalog
doesn't set `replication_method` to `FULL_TABLE`. Otherwise, every sync reads
every object.

S3 LastModified has one-second resolution, and a multipart upload keeps the
time it started. So an object can appear after the tap has bookmarked a later
time. To read such objects, the tap keeps a lookback window:

- The bookmark never passes the listing time minus `lookback_minutes`.
- Objects read inside the window are kept in state as a map of key to ETag.
  The next sync lists the window again and skips only those objects.
- An object whose content changes gets a new ETag, so the tap reads it again.
- The tap prunes entries at or before the bookmark, using the LastModified
  values from the current listing, so the state stays small.
- The tap skips objects at or before the bookmark.
- State written before the window existed has no `window` key. The first
  sync lowers its bookmark once to the window start.

The tap writes STATE after 30 seconds, or after a number of objects equal to
a tenth of the window, with a minimum of 100. So the bytes of STATE written
grow linearly with the number of objects. It also writes STATE at the end of
each stream, and before it stops on an error.

The bookmark moves after the last row of each object. When several objects
share one LastModified value, it moves after the last of them. At the end of a
stream, it moves to the start of the window.

An upload that takes longer than `lookback_minutes` can still be missed. If
your uploads take longer, raise the setting.

### Failures

The tap pins every read of an object to the ETag from the listing, using
`IfMatch`. This covers reopening a file, such as the second pass over a JSON
wrapper or an encoding fallback, and each ranged read of a Parquet file. If
an object changes during the sync, the stream fails with a message that says
so. The next run reads the new version.

The tap stops the sync on any object it can't parse. The error names the
`s3://` address and the parse error, so the Hotglue job log shows the file.
To leave a file out on purpose, use `exclude_pattern`.

A JSON value can also break its catalog type after sampling. For example, row
1,101 of a JSONL file holds `"N/A"` in a column that the sample typed as
`integer`. The tap stops the sync. The error names the object, the row, the
column and the value. To sync the object, fix the file or change the column's
type in the catalog.

A catalog saved before delimited columns became text can still type a CSV
column as `integer`. The tap applies that type, and a value that doesn't fit
fails the same way. To fix it, save the catalog again from a new discovery.

## Development

```bash
poetry install
poetry run tap-s3 --about
poetry run pytest
```

The tests use [moto](https://github.com/getmoto/moto) to mock S3. `pytest`
reports coverage for `tap_s3/` and fails under 90%.

To run discovery against a real bucket, put the settings in `config.json`:

```bash
poetry run tap-s3 --config config.json --discover > catalog.json
poetry run tap-s3 --config config.json --catalog catalog.json
```

## CI

`.github/workflows/ci.yml` defines one job named `CI`. The org ruleset
requires it on every pull request to `main`. Don't rename it, because the
ruleset matches the literal string `CI`.
