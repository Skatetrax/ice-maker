import os
import sys

_db_url = os.environ.get("ICEMAKER_DB_URL")
if not _db_url:
    print(
        "ERROR: ICEMAKER_DB_URL is not set.\n"
        "  Export it before running ice-maker, e.g.:\n"
        "    export ICEMAKER_DB_URL=postgresql://user:pass@host:5432/icemaker\n"
        "  See .env.example for all available environment variables.",
        file=sys.stderr,
    )
    sys.exit(1)

DATABASE_URL = _db_url

SKATETRAX_DB_URL = os.environ.get("SKATETRAX_DB_URL", "")
SKATETRAX_API_URL = os.environ.get(
    "SKATETRAX_API_URL",
    "https://api.skatetrax.com/api/v4/public/rinks",
)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_RATE_LIMIT = 1.0  # seconds between requests
NOMINATIM_USER_AGENT = "ice-maker/0.1 (skatetrax rink directory builder)"

GEOCODE_CONFIDENCE_THRESHOLD = 0.7
FUZZY_NAME_THRESHOLD = 0.8
FUZZY_NAME_THRESHOLD_NO_STREET = 0.6
GEO_PROXIMITY_MILES = 0.5
