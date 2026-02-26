import time
import logging
import requests

logger = logging.getLogger(__name__)

LTS_URL = 'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch'
STATE_COUNT = 50
REQUEST_DELAY = 0.5


def _build_session():
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "ice-maker/0.1 (skatetrax rink directory builder)",
    })
    return session


def pull_lts_data(session, state_id):
    """Fetch programs for a single state ID. Returns a list of dicts."""
    payload = f'facilityName=&stateId={state_id}&zip=&radius=2000'

    try:
        resp = session.post(LTS_URL, data=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('programs', [])
    except requests.RequestException as e:
        logger.warning("learntoskate: state %d request failed: %s",
                       state_id, e)
        return []
    except (ValueError, KeyError) as e:
        logger.warning("learntoskate: state %d bad response: %s",
                       state_id, e)
        return []


def aggr_lts():
    """Aggregate programs from all 50 states.

    Returns a list of dicts with keys:
        name, street, city, state, zip, lat, lng, website, phone, org_type
    """
    session = _build_session()
    results = []

    for state_id in range(1, STATE_COUNT + 1):
        programs = pull_lts_data(session, state_id)
        logger.info("learntoskate: state %d/%d → %d programs",
                    state_id, STATE_COUNT, len(programs))

        for prog in programs:
            org_name = (prog.get('OrganizationName') or '').strip()
            street = (prog.get('StreetOne') or '').strip()
            city = (prog.get('City') or '').strip()
            state = (prog.get('StateCode') or '').strip()
            postal = (prog.get('PostalCode') or '').strip()

            if not street or not city or not state:
                logger.debug("learntoskate: skipping incomplete entry: %s",
                             org_name or '(no name)')
                continue

            zip5 = postal.split('-')[0] if postal else ''

            lat_raw = prog.get('Lat') or prog.get('Latitude')
            lng_raw = prog.get('Lng') or prog.get('Longitude')
            lat = float(lat_raw) if lat_raw else None
            lng = float(lng_raw) if lng_raw else None

            results.append({
                'name': org_name,
                'street': street,
                'city': city,
                'state': state,
                'zip': zip5,
                'lat': lat,
                'lng': lng,
                'website': (prog.get('Website') or '').strip(),
                'phone': (prog.get('FacilityPhoneNumber') or
                          prog.get('OrganizationPhoneNumber') or '').strip(),
                'org_type': (prog.get('OrganizationType') or '').strip(),
            })

        if state_id < STATE_COUNT:
            time.sleep(REQUEST_DELAY)

    logger.info("learntoskate: %d total programs collected", len(results))
    return results
