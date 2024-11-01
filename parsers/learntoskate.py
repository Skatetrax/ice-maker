import requests
import csv


def pull_lts_data(stateID):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    url = 'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch'

    raw_data = f'facilityName=&stateId={stateID}&zip=&radius=2000'

    r = requests.post(url, headers=headers, data=raw_data)

    data = r.json()
    results = data['programs']

    return results


def aggr_lts():
    rinks = []
    for i in range(50):
        stateID = i + 1
        state_data = pull_lts_data(stateID)
        for rink in state_data:
            update_rink = {"Name": ' '}
            update_rink.update(rink)
            rinks.append(update_rink)

    return rinks


def lts_csv(path):

    rinks = aggr_lts()

    with open(path, 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(
            output_file,
            extrasaction='ignore',
            delimiter=';',
            fieldnames=['Name', 'StreetOne', 'City', 'StateCode']
            )
        fc.writerows(rinks)
