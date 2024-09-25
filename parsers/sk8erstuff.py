import bs4
import requests
#import usaddress
import common # for debug and testing purposes
from collections import OrderedDict


def pull_sk8trstuff(state):
    rinks = []
    url = 'http://sk8stuff.com/utility/lister_rinks.asp?stap={}'.format(state)
    req = requests.get(url)
    #req.status_code
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
        rink = {'name': rink_name, 'street': rink_street, 'city': rink_city, 'state': state, 'phone': rink_phone}
        rinks.append(rink)

    return rinks

