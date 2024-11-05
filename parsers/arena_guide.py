from tqdm import tqdm
import bs4
import requests
import json
import csv
import re


def arena_guide_request(page_number):
    '''
    This function is used to build the request dynamically.
    '''

    url = r"https://www.arena-guide.com/wp-admin/admin-ajax.php?action=jet-engines/arenas-with-pagination"
    session = requests.Session()
    session.get("https://www.arena-guide.com/locations/usa")

    formdata = {
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
        'paged': [page_number],
        'referrer[uri]': ['/locations/usa/'],
        'referrer[info]': '',
        'referrer[self]': ['/index.php']
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    r = session.post(url, headers=headers, data=formdata)
    content = json.loads(r.text)

    return content


def pull_arena_guide_pages():
    '''
    First, we need to send a request to get a total number
    of pages in the pagination settings
    '''

    page_number = 1
    data = arena_guide_request(page_number)

    return int(data['pagination']['found_posts'])


def pull_arena_guide_content():
    '''
    Take the amount of pages we have, and build a loop
    For each page, build a list of names and addresses
    Appened a dict with name and address on every rink
    Return a list of dicts
    '''

    page_number = 20
    pages = pull_arena_guide_pages()
    rinks = []
    rink_names = []
    rink_addresses = []

    for i in tqdm(range(pages)):
        page_number = i + 1
        content = arena_guide_request(page_number)
        soup = bs4.BeautifulSoup(content['content'], "lxml")
        main = soup.find_all('div', class_="jet-listing-grid jet-listing")

        for entry in main:
            # returns a list of rink names with html
            rink_name = entry.find_all('h2')
            # retuns a list of rink addresses with html
            rink_address = entry.find_all('span', class_="elementor-icon-list-text")

            # if the find_all failed, the text.strip will fail
            # for now, we dont care.
            for rink in rink_name:
                if rink is None:
                    pass
                else:
                    rink_names.append(rink.text.strip())

            for addr in rink_address:
                if addr is None:
                    pass
                else:
                    # remove unneeded, inconsitent country and zip data
                    # these are/should be unique to the arena-guide data source
                    location = addr.text.strip()
                    location = location.removesuffix("United States of America").strip()
                    location = location.removesuffix("United States").strip()
                    location = location.removesuffix("USA").strip()
                    location = re.sub(r"\s?\d+$", "", location).strip()
                    location = location.rstrip(',')
                    # knock out any website URL's for now
                    if 'http' not in location:
                        rink_addresses.append(location)

    #get the sizes of both name/address lists for comparing
    rink_names_size = len(rink_names)
    rink_addresses_size = len(rink_addresses)
    if rink_names_size == rink_addresses_size:
        for x in range(rink_names_size):
            rinks.append({'name': rink_names[x], 'address': rink_addresses[x]})

    return rinks


def arena_guide_csv(path):
    '''
    produces a csv file of the data pulled directly from sk8stuff.
    use this file as a cache or direct data processing
    '''
    data = pull_arena_guide_content()

    with open(path, 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(
            output_file,
            fieldnames=data[0].keys(),
            delimiter=';'
            )
        fc.writerows(data)