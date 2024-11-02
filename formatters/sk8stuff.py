import pandas as pd
import usaddress
import numpy as np
from utils import common


def address_formatter(x):
    # keep x for error reporting, use address for
    # processing/assembling data
    address = x

    try:
        address = usaddress.tag(address)
        address = address[0]
        street = address['StreetName'] + ' ' + address['StreetNamePostType']
        results = {'street': street}
    except:
        print("failed to parse:", x)
        results = {'street': np.nan}

    return results


def process_sk8stuff():
    csv_data = '/tmp/ice-maker_raw_csv_sk8stuff.csv'

    # Load the data of csv
    df = pd.read_csv(csv_data,
                    sep=';',
                    engine='python',
                    names=["Name", "street", "city", "state"])

    # drop any obvious dupes, they're going to happen
    # and apply some normalization to the address section
    df['street'] = df['street'].map(common.country_us._lookup_words)
    df = df.drop_duplicates()
    df['street'] = df['street'].apply(address_formatter)
    df['street'] = df.apply(lambda row: row.street['street'], axis=1)

    # remove any row w/o all fields preset (because they failed to parse)
    df = df.dropna()
    # delete the old address blob to clean up & drop any remaining dupes
    df = df.drop_duplicates()

    return df
