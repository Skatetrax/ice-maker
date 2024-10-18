import bs4
import requests


def pull_arenaguide():
    '''
    Arena-Guide can give us a rink name, address, and url.
    Also its NHL Affiliation.
    This site is currently paginated :(
    This will currently only scrape the first page

    This will currently only return street addresses,
    validate other information against google maps api
    '''

    rinks = []
    url = 'https://arena-guide.com/locations/usa/'
    req = requests.get(url)
    soup = bs4.BeautifulSoup(req.text, "lxml")
    main = soup.find_all('div', class_="jet-listing-grid jet-listing")

    for entry in main:
        # rink_name = entry.find_all('h2') # returbs name
        addresses = entry.find_all('span', class_="elementor-icon-list-text")
        for i in addresses:
            location = i.text.strip()
            if 'http' not in location:
                rinks.append(location)
    return rinks
