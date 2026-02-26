# ice-maker

An open-source data pipeline that builds and maintains a directory of US ice rinks by aggregating data from multiple online sources, verifying addresses through geocoding, and deduplicating entries across sources.

Built for [Skatetrax](https://github.com/asyoung/skatetrax), a training tracker for figure skaters. Skatetrax uses this directory so skaters can log sessions at real rinks with stable UUIDs -- if a rink is renamed or closed, the UUID stays the same.

## Data sources

| Source | Type | What it provides |
|--------|------|------------------|
| [arena-guide.com](https://www.arena-guide.com) | Web scrape (paginated CMS) | Name + address for ~1,800 venues |
| [sk8stuff.com](https://sk8stuff.com) | Web scrape (single page) | Name + street + city/state |
| [learntoskateusa.com](https://www.learntoskateusa.com) | JSON API | Name + address + coordinates + zip |
| [Fandom wiki](https://figure-skating.fandom.com/wiki/List_of_ice_rinks_in_the_USA) | MediaWiki API | Name + city + state + defunct status + clubs |

## Quick start (Podman)

```bash
# 1. Build the image
podman build -t ice-maker -f Containerfile .

# 2. Start a Postgres instance (or point to your existing one)
podman run -d --name icemaker-pg \
  -e POSTGRES_DB=icemaker \
  -e POSTGRES_USER=icemaker \
  -e POSTGRES_PASSWORD=icemaker \
  -p 5432:5432 \
  postgres:17

# 3. Run all sources and export a CSV
podman run --rm \
  -e ICEMAKER_DB_URL=postgresql://icemaker:icemaker@host.containers.internal:5432/icemaker \
  -v ./output:/output \
  ice-maker --source all --export-csv /output/rinks.csv
```

## Quick start (local dev)

```bash
# Install dependencies
pip install pipenv
pipenv install --dev

# Set the database URL
export ICEMAKER_DB_URL=postgresql://user:pass@localhost:5432/icemaker

# Run a single source
python run_pipeline.py --source sk8stuff

# Run everything: scrape all sources, geocode, promote
python run_pipeline.py --source all

# Export locations to CSV
python run_pipeline.py --export-csv rinks.csv

# Combine: run pipeline then export
python run_pipeline.py --source all --export-csv rinks.csv
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ICEMAKER_DB_URL` | Yes | PostgreSQL connection string for the ice-maker database |
| `SKATETRAX_DB_URL` | No | PostgreSQL connection string for the Skatetrax database (needed for `--push-to-skatetrax` and `--sync-ice-time`) |
| `SKATETRAX_API_URL` | No | Public rink API for UUID alignment during promotion (defaults to `https://api.skatetrax.com/api/v4/public/rinks`) |

See `.env.example` for a template.

## CLI reference

### run_pipeline.py

Pipeline operations (mutually exclusive):

| Flag | Description |
|------|-------------|
| `--source NAME` | Run full pipeline for a source (`sk8stuff`, `arena_guide`, `learntoskate`, `fandom_wiki`, or `all`) |
| `--geocode-pending` | Geocode existing unverified candidates without re-scraping |
| `--promote` | Promote verified candidates to the locations table |
| `--sync-ice-time` | Confirm rinks via Skatetrax ice_time records (requires `SKATETRAX_DB_URL`) |
| `--repair-failed` | Re-parse geocode_failed candidates with the fixed address parser |
| `--push-to-skatetrax` | Push verified locations into the Skatetrax database (requires `SKATETRAX_DB_URL`) |

Additional flags:

| Flag | Description |
|------|-------------|
| `--export-csv PATH` | Export the locations table to CSV (can combine with any operation above, or run standalone) |
| `--dry-run` | Preview `--push-to-skatetrax` changes without writing |
| `--scrape-only` | Only scrape and fingerprint, skip parsing and geocoding |
| `--no-geocode` | Skip the geocoding step |
| `--geocode-source NAME` | Limit `--geocode-pending` to candidates from a specific source |
| `--limit N` | Process at most N new entries (useful for testing) |

### manage_locations.py

Manual data management:

```bash
# Change a rink's status
python manage_locations.py demote --name "Crown Coliseum" --status seasonal

# Merge duplicate locations
python manage_locations.py merge --from-rink UUID_A --into-rink UUID_B

# Rename a rink (old name becomes an alias)
python manage_locations.py rename --rink-id UUID --new-name "New Name"

# Search by name
python manage_locations.py search "Polar" --state NC
```

## Pushing data to Skatetrax

The `--push-to-skatetrax` flag upserts ice-maker's verified locations into the Skatetrax database.

Safety guarantees:
- Existing Skatetrax rinks are never deleted.
- Curated fields (`rink_name`, `rink_phone`, `rink_url`, `rink_tz`) are never overwritten on existing entries.
- When ice-maker has a different name for an existing rink, it is recorded as an alias in `location_aliases` for future "aka / formerly known as" use.
- Use `--dry-run` to preview changes before committing.

```bash
# Preview what would happen
python run_pipeline.py --push-to-skatetrax --dry-run

# Push for real
python run_pipeline.py --push-to-skatetrax
```

## Deploying to Kubernetes

```bash
# 1. Create the secret (copy and edit the example first)
cp k8s/secret.yaml.example k8s/secret.yaml
# Edit k8s/secret.yaml with real credentials
kubectl apply -f k8s/secret.yaml

# 2. Deploy the CronJob (runs daily at 4 AM UTC)
kubectl apply -f k8s/cronjob.yaml

# 3. Trigger a manual run
kubectl create job --from=cronjob/ice-maker manual-run-$(date +%s)

# 4. Change the schedule
kubectl patch cronjob ice-maker -p '{"spec":{"schedule":"0 6 * * *"}}'

# 5. Suspend/resume
kubectl patch cronjob ice-maker -p '{"spec":{"suspend":true}}'
kubectl patch cronjob ice-maker -p '{"spec":{"suspend":false}}'
```

For per-source scheduling, create additional CronJobs with different `args` and `schedule` values:

```yaml
# Example: run arena-guide weekly on Sundays at 2 AM
args: ["--source", "arena_guide"]
schedule: "0 2 * * 0"
```

## Running tests

```bash
pipenv install --dev
export ICEMAKER_DB_URL=sqlite://
python -m pytest tests/ -v --cov=pipeline --cov=parsers --cov=utils --cov-report=term-missing
```

Tests use an in-memory SQLite database and mocked HTTP responses -- no network access or external database required.

## Project structure

```
ice-maker/
  config.py              # Environment-based configuration
  run_pipeline.py        # Main CLI entrypoint
  manage_locations.py    # Manual location management CLI
  Containerfile          # Container image definition
  k8s/                   # Kubernetes manifests
  parsers/               # Source-specific scrapers
  pipeline/              # Core pipeline: staging models, fingerprint,
                         #   geocoder, matcher, promoter, demoter,
                         #   skatetrax_push
  formatters/            # Legacy address formatters
  utils/                 # Shared utilities (normalization, state codes)
  tests/                 # pytest suite with fixtures
```

## License

GNU General Public License v2.0 -- see [LICENSE](LICENSE).
