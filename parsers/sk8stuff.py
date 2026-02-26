import requests
import bs4
import csv
import logging

logger = logging.getLogger(__name__)

SK8STUFF_URL = 'https://sk8stuff.com/utility/lister_rinks.php'


def pull_sk8stuff():
    '''
    Fetch the full rink list from sk8stuff in a single request.
    The PHP page returns every rink in one HTML table with columns:
    Rink Name | Street | City/State/Zip | Rink Phone | Map
    '''
    rinks = []

    req = requests.get(SK8STUFF_URL)
    req.raise_for_status()

    soup = bs4.BeautifulSoup(req.text, 'html.parser')
    tables = soup.find_all('table')

    if not tables:
        logger.warning("No table found at %s", SK8STUFF_URL)
        return rinks

    rows = tables[0].find_all('tr')
    skipped = 0

    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 3:
            skipped += 1
            continue

        rink_name = cells[0].text.strip().replace(';', ' -').replace(',', ' -')
        rink_street = cells[1].text.strip().replace(',', ' ').replace("\n", " ")
        city_state = cells[2].text.strip()

        # "City/State/Zip" column is typically "CityName ST" or "CityName ST 12345"
        parts = city_state.rsplit(' ', 1)
        if len(parts) == 2:
            rink_city, rink_state = parts
        else:
            rink_city = city_state
            rink_state = ''

        if not rink_name or 'Junk Rink' in rink_name:
            skipped += 1
            continue

        rinks.append({
            'name': rink_name,
            'street': rink_street,
            'city': rink_city,
            'state': rink_state
        })

    logger.info("sk8stuff: %d rinks collected, %d rows skipped", len(rinks), skipped)
    return rinks


def sk8stuff_csv(path):
    '''
    Produces a csv file of the data pulled directly from sk8stuff.
    Use this file as a cache or direct data processing.
    '''
    data = pull_sk8stuff()

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
