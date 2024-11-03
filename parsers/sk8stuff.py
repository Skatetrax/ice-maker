from utils import common
import requests
import bs4
import csv


def pull_sk8stuff(state):
    '''
    this function takes an abbreviate state name and searches sk8erstuff
    each result is added as an element to the 'rink' list
    function returns a list of dicts - every rink in the provide state
    '''

    rinks = []
    url = 'http://sk8stuff.com/utility/lister_rinks.asp?stap={}'.format(state)
    req = requests.get(url)
    # req.status_code
    soup = bs4.BeautifulSoup(req.text, 'html.parser')

    table = soup.find_all('table')[0]
    rows = table.find_all('tr')

    for row in rows[2:]:
        # replace and 'fix' wierd characters
        cells = row.find_all('td')
        name = cells[0].text
        street = cells[1].text
        rink_name = name.strip().replace(';', ' -').replace(',', ' -')
        rink_street = street.strip().replace(',', ' ').replace("\n", " ")
        rink_city_state = cells[2].text.strip()
        rink_city = rink_city_state.rsplit(' ', 1)[0]
        rink = {'name': rink_name,
                'street': rink_street,
                'city': rink_city,
                'state': state}
        rinks.append(rink)

    return rinks


def aggr_sk8stuff():
    '''
    This function simply fetches all
    rinks via sk8stuff for all states
    returns lists of dicts
    '''

    rinks = []
    states = common.country_us.states
    for state in states:
        rinks.append(pull_sk8stuff(state))

    return rinks


def sk8stuff_csv(path):
    '''
    produces a csv file of the data pulled directly from sk8stuff.
    use this file as a cache or direct data processing
    '''
    data = aggr_sk8stuff()

    # flatten the results
    master_rink_list = []
    for y in data:
        for x in y:
            # sk8stuff puts a dummy rink in the data
            # so remove it.
            if 'Junk Rink' in x['name']:
                pass
            else:
                master_rink_list.append(x)

    with open(path, 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(
            output_file,
            fieldnames=master_rink_list[0].keys(),
            delimiter=';'
            )
        fc.writerows(master_rink_list)
