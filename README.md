# tap-template

Template for new Singer taps. Click **Use this template** (top-right on GitHub) to bootstrap a new tap.

## What you get out of the box

- Meltano-SDK tap scaffold (`tap_template/tap.py`, `client.py`, `streams.py`)
- Poetry-based packaging (`pyproject.toml`)
- A smoke test (`tests/test_tap.py`)
- GitHub Actions workflow named `CI` — already satisfies the org's required status check
- Standard Python `.gitignore`

The repo is created with the org-wide branch protection ruleset applied automatically (squash-only merges, 1 required review, `CI` must pass).

## Customizing for a new tap

After creating your repo from this template, find-and-replace these tokens:

| Find | Replace with |
|---|---|
| `tap-template` | `tap-yourthing` (kebab-case package name) |
| `tap_template` | `tap_yourthing` (snake_case module name) |
| `TapTemplate` | `TapYourThing` (PascalCase class name) |
| `TemplateStream` | `YourThingStream` (PascalCase base stream) |

Then rename the `tap_template/` directory to match.

One-liner that does all of the above (run from repo root):

```bash
NAME=yourthing  # set this
KEBAB="tap-$NAME"
SNAKE="tap_$NAME"
PASCAL="Tap$(echo "$NAME" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')"

find . -type f \( -name '*.py' -o -name '*.toml' -o -name '*.yml' -o -name '*.md' \) \
  -not -path './.git/*' -exec sed -i '' \
  -e "s/tap-template/$KEBAB/g" \
  -e "s/tap_template/$SNAKE/g" \
  -e "s/TapTemplate/$PASCAL/g" {} +

mv tap_template "$SNAKE"
```

## Customizing the tap

1. Edit `tap_yourthing/tap.py` — declare config schema (API keys, start dates, etc.)
2. Edit `tap_yourthing/client.py` — set `url_base`, auth headers, pagination
3. Edit `tap_yourthing/streams.py` — one class per resource. Set `name`, `path`, `primary_keys`, `replication_key`, `schema`
4. Add a real test to `tests/test_tap.py` that hits a fixture or recorded response

## Local development

```bash
poetry install
poetry run tap-yourthing --help
poetry run pytest
```

## CI

`.github/workflows/ci.yml` defines a single job named `CI` that the org ruleset requires on every PR to `main`. Don't rename it — the ruleset matches on the literal string `CI`.
