import bs4
import requests
import json
import csv
import re
import logging
import time

logger = logging.getLogger(__name__)

POST_URL = "https://www.arena-guide.com/wp-admin/admin-ajax.php?action=jet-engines/arenas-with-pagination"
SEED_URL = "https://www.arena-guide.com/locations/usa"

REQUEST_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:60.0) Gecko/20100101 Firefox/60.0"
}

FORM_TEMPLATE = {
    'action': ['jet_smart_filters'],
    'provider': ['jet-engine/arenas-with-pagination'],
    'settings[lisitng_id]': ['40'],
    'settings[columns]': ['2'],
    'settings[columns_tablet]': '',
    'settings[columns_mobile]': ['1'],
    'settings[column_min_width]': ['240'],
    'settings[column_min_width_tablet]': '',
    'settings[column_min_width_mobile]': '',
    'settings[inline_columns_css]': ['false'],
    'settings[is_archive_template]': ['yes'],
    'settings[post_status][]': ['publish'],
    'settings[use_random_posts_num]': '',
    'settings[posts_num]': ['6'],
    'settings[max_posts_num]': ['9'],
    'settings[not_found_message]': ['No data was found'],
    'settings[is_masonry]': '',
    'settings[equal_columns_height]': '',
    'settings[use_load_more]': '',
    'settings[load_more_id]': '',
    'settings[load_more_type]': ['click'],
    'settings[load_more_offset][unit]': ['px'],
    'settings[load_more_offset][size]': ['0'],
    'settings[loader_text]': '',
    'settings[loader_spinner]': '',
    'settings[use_custom_post_types]': '',
    'settings[hide_widget_if]': '',
    'settings[carousel_enabled]': '',
    'settings[slides_to_scroll]': ['1'],
    'settings[arrows]': ['true'],
    'settings[arrow_icon]': ['fa fa-angle-left'],
    'settings[dots]': '',
    'settings[autoplay]': ['true'],
    'settings[pause_on_hover]': ['true'],
    'settings[autoplay_speed]': ['5000'],
    'settings[infinite]': ['true'],
    'settings[center_mode]': '',
    'settings[effect]': ['slide'],
    'settings[speed]': ['500'],
    'settings[inject_alternative_items]': '',
    'settings[scroll_slider_enabled]': '',
    'settings[scroll_slider_on][]': ['desktop'],
    'settings[scroll_slider_on][]': ['tablet'],
    'settings[scroll_slider_on][]': ['mobile'],
    'settings[custom_query]': ['yes'],
    'settings[_element_id]': ['arenas-with-pagination'],
    'props[found_posts]': ['1773'],
    'props[max_num_pages]': ['89'],
    'props[page]': ['1'],
    'props[query_type]': ['posts'],
    'props[query_id]': ['s'],
    'referrer[uri]': ['/locations/usa/'],
    'referrer[info]': '',
    'referrer[self]': ['/index.php']
}


def _build_session():
    '''Create a single session seeded with the initial GET.'''
    session = requests.Session()
    session.get(SEED_URL)
    return session


def arena_guide_request(session, page_number, pagination_props=None):
    '''
    This function is used to build the request dynamically.
    Accepts an existing session to avoid redundant connections.

    pagination_props, when provided, overrides the hardcoded
    found_posts / max_num_pages values in the form data so that
    subsequent requests reflect what the server actually reported.
    '''
    formdata = dict(FORM_TEMPLATE)
    formdata['paged'] = [page_number]

    if pagination_props:
        formdata['props[found_posts]'] = [str(pagination_props['found_posts'])]
        formdata['props[max_num_pages]'] = [str(pagination_props['max_num_pages'])]

    r = session.post(POST_URL, headers=REQUEST_HEADERS, data=formdata)
    r.raise_for_status()
    content = json.loads(r.text)

    return content


def pull_arena_guide_pages(session):
    '''
    Send an initial request to discover the live post count and
    page ceiling from the server. Returns both as a dict so
    subsequent requests can forward them.
    '''
    data = arena_guide_request(session, 1)
    pagination = data['pagination']

    props = {
        'found_posts': int(pagination['found_posts']),
        'max_num_pages': int(pagination['max_num_pages']),
    }

    logger.info(
        "Server reports found_posts=%d, max_num_pages=%d",
        props['found_posts'], props['max_num_pages']
    )

    return props


def _clean_address(raw_text):
    '''Strip trailing country names, zip codes, and URLs from an address.'''
    location = raw_text.strip()
    location = location.removesuffix("United States of America").strip()
    location = location.removesuffix("United States").strip()
    location = location.removesuffix("USA").strip()
    location = re.sub(r"\s?\d+$", "", location).strip()
    location = location.rstrip(',')
    if 'http' in location:
        return None
    return location


REQUEST_DELAY = 0.5  # seconds between requests to respect site owner


def pull_arena_guide_content():
    '''
    Fetch every page from arena-guide and extract rink entries.

    The API returns ~10 rink cards per rendered page inside a grid
    container.  Individual cards are div.jet-listing-grid__item
    elements, each with one h2 (name) and two spans (address + URL).

    The server-reported max_num_pages is based on a different internal
    page size, so we paginate until we receive an empty page rather
    than trusting that value.
    '''
    session = _build_session()
    pagination_props = pull_arena_guide_pages(session)
    expected_posts = pagination_props['found_posts']
    rinks = []
    failed_pages = []
    consecutive_empty = 0
    skipped_cards = 0
    page_number = 0

    logger.info("Arena-Guide reports %d posts; paginating until empty",
                expected_posts)

    while True:
        page_number += 1

        try:
            content = arena_guide_request(session, page_number, pagination_props)
        except requests.RequestException as e:
            logger.warning("Request failed for page %d: %s", page_number, e)
            failed_pages.append(page_number)
            consecutive_empty += 1
            if consecutive_empty >= 5:
                logger.info("5 consecutive empty/failed pages, stopping")
                break
            time.sleep(REQUEST_DELAY * 4)
            continue

        soup = bs4.BeautifulSoup(content['content'], "lxml")
        cards = soup.find_all('div', class_='jet-listing-grid__item')

        if not cards:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logger.info("3 consecutive empty pages at page %d, stopping",
                            page_number)
                break
            time.sleep(REQUEST_DELAY)
            continue

        consecutive_empty = 0

        for card in cards:
            name_tag = card.find('h2')
            addr_spans = card.find_all('span', class_='elementor-icon-list-text')

            name = name_tag.text.strip() if name_tag else None
            address = None
            for span in addr_spans:
                cleaned = _clean_address(span.text)
                if cleaned:
                    address = cleaned
                    break

            if name and address:
                rinks.append({'name': name, 'address': address})
            else:
                skipped_cards += 1
                logger.debug(
                    "Page %d card skipped: name=%r, address=%r",
                    page_number, name,
                    addr_spans[0].text[:60] if addr_spans else None
                )

        if page_number % 20 == 0:
            logger.info("Progress: page %d (%d rinks so far)",
                        page_number, len(rinks))

        time.sleep(REQUEST_DELAY)

    if failed_pages:
        logger.warning("%d pages failed: %s", len(failed_pages), failed_pages)
    if skipped_cards:
        logger.info("%d cards skipped (missing name or address)", skipped_cards)

    logger.info("Collected %d rinks from %d pages (expected %d)",
                len(rinks), page_number, expected_posts)

    return rinks


def arena_guide_csv(path):
    '''
    Produces a csv file of the data pulled directly from arena-guide.
    Use this file as a cache or direct data processing.
    '''
    data = pull_arena_guide_content()

    if not data:
        logger.warning("No data returned, skipping CSV write")
        return

    with open(path, 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(
            output_file,
            fieldnames=data[0].keys(),
            delimiter=';'
            )
        fc.writerows(data)