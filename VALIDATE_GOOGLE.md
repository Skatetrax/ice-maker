# Google Places Validation Tool

One-time bulk validation of ice-maker's rink directory against the
[Google Places API (New)](https://developers.google.com/maps/documentation/places/web-service/op-overview).
The goal is to produce a CSV report that flags name mismatches, permanently
closed venues, and non-ice-rink entries so Skatetrax can clean up its
authoritative directory in a single pass.

This is **not** part of the recurring pipeline. Run it once (or occasionally)
with a Google Cloud free-trial key, review the report, and cancel the trial.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.13+ | Same as the rest of ice-maker |
| `ICEMAKER_DB_URL` | Connection string to the ice-maker database (Postgres or SQLite) |
| `GOOGLE_PLACES_API_KEY` | API key with **Places API (New)** and **Time Zone API** enabled |

### Getting a Google API key

1. Create a [Google Cloud account](https://cloud.google.com/free) (new accounts
   get **$300 in free credits for 90 days**).
2. Create a new project (e.g. `skatetrax-validation`).
3. Enable two APIs in the project:
   - **Places API (New)** -- for name, address, status, phone, website lookups
   - **Time Zone API** -- for resolving lat/lng to IANA timezone names
4. Go to **APIs & Services > Credentials** and create an API key.
5. (Recommended) Restrict the key to only those two APIs.

```bash
export GOOGLE_PLACES_API_KEY="AIzaSy..."
```

---

## Cost Estimate

| API | Per request | Notes |
|---|---|---|
| Places Text Search (Advanced fields) | ~$0.040 | Includes phone + website |
| Time Zone API | ~$0.005 | One call per rink with lat/lng |
| **Total per rink** | **~$0.045** | |

For ~2,100 active locations the full run costs roughly **$95**, well inside the
$300 free credit.

---

## Usage

```bash
# See what will be queried and the cost estimate (no API calls)
python validate_google.py --dry-run

# Test with a small batch first
python validate_google.py --limit 5 -o test_report.csv

# Full validation
python validate_google.py -o validation_report.csv

# Resume after an interruption (skips rinks already in the CSV)
python validate_google.py -o validation_report.csv --resume

# Slow it down if you hit rate limits (default is 0.25s between calls)
python validate_google.py -o validation_report.csv --delay 0.5
```

### CLI Options

| Flag | Default | Description |
|---|---|---|
| `--output`, `-o` | `validation_report.csv` | Path for the output CSV |
| `--dry-run` | off | Print queries and cost estimate without calling Google |
| `--resume` | off | Append to an existing CSV, skipping already-validated rinks |
| `--limit N` | 0 (all) | Only validate the first N locations |
| `--delay N` | 0.25 | Seconds to wait between API calls |

---

## Output CSV

Each row compares one ice-maker location against Google's top search result.

### ice-maker fields (what we have)

| Column | Description |
|---|---|
| `rink_id` | UUID from the ice-maker locations table |
| `im_name` | Current rink name in ice-maker |
| `im_address` | Street address |
| `im_city` | City |
| `im_state` | Two-letter state code |
| `im_zip` | ZIP/postal code |
| `im_phone` | Phone number (if any) |
| `im_url` | Website URL (if any) |
| `im_tz` | Timezone (if any) |

### Google fields (what they say)

| Column | Description |
|---|---|
| `google_place_id` | Google's unique Place ID |
| `google_name` | Business name per Google |
| `google_address` | Formatted address per Google |
| `google_status` | `OPERATIONAL`, `CLOSED_TEMPORARILY`, or `CLOSED_PERMANENTLY` |
| `google_types` | Pipe-delimited list of place types (e.g. `ice_skating_rink\|sports_complex`) |
| `google_phone` | Phone number on file with Google |
| `google_website` | Website URL on file with Google |
| `google_lat` | Latitude |
| `google_lon` | Longitude |
| `google_tz` | IANA timezone (e.g. `America/New_York`) |

### Computed fields (what to look at)

| Column | Description |
|---|---|
| `name_similarity` | 0.0 - 1.0 fuzzy match score between `im_name` and `google_name` |
| `name_match` | `True` if similarity >= 0.8 |
| `flag_closed` | `True` if Google reports the business as closed |
| `flag_not_ice` | `True` if Google's types don't include any ice/rink-related category |
| `flag_name_diff` | `True` if similarity < 0.6 (likely a different business or major rename) |
| `flag_no_result` | `True` if Google returned zero results for the query |

---

## Reading the Report

Open the CSV in a spreadsheet and filter on the flag columns. The three
actionable buckets are:

### 1. Closures (`flag_closed = True`)

These rinks should be marked `closed_permanently` (or `closed_temporarily`)
in ice-maker using `manage_locations.py`:

```bash
python manage_locations.py disable <rink_id> --reason "Google reports closed"
```

### 2. Not ice rinks (`flag_not_ice = True`)

Google didn't categorize these as anything ice-related. Manually verify --
some may be community centers that happen to have ice. Obvious false positives
(roller rinks, bowling alleys) can be disabled:

```bash
python manage_locations.py disable <rink_id> --reason "Not an ice rink"
```

### 3. Name mismatches (`flag_name_diff = True`)

Review these side by side. If the Google name is correct and the rink was
simply renamed, update it in Skatetrax (the authority) and the old name will
be recorded as an alias automatically on the next pipeline push.

### Bonus: fill in the blanks

For rinks where ice-maker has no phone, website, or timezone but Google does,
the report gives you the values to backfill. This can be done manually or
scripted as a follow-up.

---

## How It Works

```
┌─────────────────┐
│  ice-maker DB   │  Load active locations
│  (locations)    │  sorted by state, city
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Google Places   │  POST /v1/places:searchText
│  Text Search     │  query = "name, address, city, state, zip"
│  (Advanced)      │  fields: name, status, types, phone, website, lat/lng
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Google Time     │  GET /maps/api/timezone/json
│  Zone API        │  location = lat,lng
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Compare &       │  Fuzzy name match, flag closures,
│  Flag            │  flag non-ice types, flag no-results
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CSV Report      │  One row per rink, flushed after each
│                  │  (safe to interrupt and --resume)
└─────────────────┘
```

---

## Ice-related Type Detection

The script considers a Google result to be ice-related if its `types` array
contains at least one of:

- `ice_skating_rink`
- `skating_rink`
- `sports_complex`
- `stadium`
- `arena`
- `sports_club`
- `fitness_center`
- `recreation_center`

This is intentionally broad to avoid false negatives. The `flag_not_ice`
column catches entries that don't match **any** of these, which are the ones
worth manual review.

---

## Tips

- **Always `--dry-run` first** to sanity-check the query count and cost.
- **Start with `--limit 5`** to verify the API key works and the results look
  reasonable before committing to the full run.
- **Use `--resume`** liberally. The script flushes every row to disk, so if it
  crashes or you kill it, just re-run with `--resume` and it picks up where it
  left off.
- The output CSV is `.gitignore`d by default. Keep it local or archive it
  somewhere -- it contains your validation snapshot, not something that needs
  to live in the repo.
- After acting on the report, re-run the regular pipeline
  (`python run_pipeline.py --source all --push-to-skatetrax`) to propagate
  any status changes or name corrections to Skatetrax.
