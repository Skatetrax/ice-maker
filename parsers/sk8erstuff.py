import bs4
import requests
# import usaddress
import utils.common as common  # for debug and testing purposes
from datetime import datetime, timedelta


def pull_sk8trstuff(state):
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
        cells = row.find_all('td')
        rink_name = cells[0].text.strip()
        rink_street = cells[1].text.strip()
        rink_city_state = cells[2].text.strip()
        rink_city = rink_city_state.rsplit(' ', 1)[0]
        rink_phone = cells[3].text.strip()
        rink = {'name': rink_name,
                'street': rink_street,
                'city': rink_city,
                'state': state,
                'phone': rink_phone}
        rinks.append(rink)

    return rinks


def add_sk8erstuff_meta_v1(rink_data):
    '''
    this function only adds more fields to sanitized rows.
    version 1 data only focuses on 'StreetName' and "StreeNamePostType"
    based matches.  We also need to stamp the row with the datasource name

    maybe better as a decorator
    '''

    now = datetime.now()
    meta = {'date_created': now, 'source': 'sk8erstuff', 'version': 1}

    # my_dict.update({'d': 4, 'e': 5})
    rink = dict(rink_data)
    new_data = rink.update(meta)

    return new_data


def aggr_sk8erstuff():
    states = common.country_us.states
    for state in states:
        rinks = pull_sk8trstuff(state)
        print(rinks)


rinks = pull_sk8trstuff('pa')
for ice in rinks:
    print(ice)
#    print(usaddress.parse(ice['street']))

# aggr_sk8erstuff()
# for state in states:
#    rinks = dicts_in_a_row([state])
#    for ice in rinks:
#        ice = ice.values()
#        print(*ice)
