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

`incremental_mode` accepts a boolean or a string, because Hotglue can send
either. The strings `true`, `yes` and `1` mean true, and `false`, `no` and
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

For a `.json` object, only a field whose every item is an object counts. The
tap reads the whole structure once before it yields any row. So a file with
two such fields fails at discovery, even when the rows come first. A second
pass then streams the chosen field.
| `.jsonl`, `.ndjson` | One JSON object per line. The tap skips blank lines. |
| `.parquet` | Parquet. The tap reads it by row group with ranged GET requests. |

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

The tap infers types conservatively. A column is `integer`, `number`,
`boolean` or `date-time` only when every non-empty sampled value parses as
that type. Otherwise it is `string`.

- Integers can't have leading zeros, so `02134` stays a string.
- Booleans are `true` or `false` in any case. `yes` and `1` aren't booleans.
- Dates and times must use ISO 8601, such as `2026-09-01` or
  `2026-09-01T12:30:00Z`. `09/01/2026` stays a string.
- A JSON string can only become `date-time`. The JSON string `"42"` stays a
  string.
- Nested JSON values become `object` or `array`, with no fixed schema.
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
`_s3_last_modified`. A source column with one of these names is renamed to
`<name>_source`, such as `_s3_key_source`, and the tap logs a warning.

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

The tap writes STATE after every 100 objects or 30 seconds, whichever comes
first. It also writes STATE at the end of each stream, and before it stops on
an error.

The bookmark moves after the last row of each object. When several objects
share one LastModified value, it moves after the last of them. At the end of a
stream, it moves to the start of the window.

An upload that takes longer than `lookback_minutes` can still be missed. If
your uploads take longer, raise the setting.

### Failures

The tap stops the sync on any object it can't parse. The error names the
`s3://` address and the parse error, so the Hotglue job log shows the file.
To leave a file out on purpose, use `exclude_pattern`.

A value can also break its catalog type after sampling. For example, row 1,101
of a file holds `N/A` in a column that the sample typed as `integer`. The
tap stops the sync. The error names the object, the row, the column and the
value. To sync the object, fix the file or change the column's type in the
catalog.

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
