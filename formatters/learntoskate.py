import pandas as pd
import usaddress
import numpy as np
from utils import common


def address_formatter(x):
    # keep x for error reporting, use address for
    # processing/assembling data
    source = 'learntoskate'
    address = x

    try:
        address = usaddress.tag(address)
        address = address[0]
        street = address['StreetName'] + ' ' + address['StreetNamePostType']
        results = {'street': street}

    except usaddress.RepeatedLabelError as error:
        error = error.parsed_string
        message = f'error parsing "{x}" from "{source}": \n {error}'
        common.ice_maker_logging.fomatter_errors(message)
        results = {'street': np.nan}

    except (KeyError, TypeError) as error:
        error = error.args
        message = f'failed to parse {x} from "{source}", "{error}"'
        common.ice_maker_logging.fomatter_errors(message)
        results = {'street': np.nan}

    return results


def process_lts():
    csv_data = '/tmp/ice-maker_raw_csv_lts.csv'

    # Load the data of csv
    df = pd.read_csv(
        csv_data,
        sep=';',
        engine='python',
        names=["name", "street", "city", "state"]
        )

    # remove any UTF-8 wierdness from WP scraping
    df['name'] = df['name'].apply(common.reset_utf8)

    # drop any obvious dupes, they're going to happen
    # and apply some normalization to the address section
    df['city'] = df['city'].apply(common.country_us._remove_punctuation)
    df['street'] = df['street'].apply(common.country_us._remove_punctuation)
    df['street'] = df['street'].map(common.country_us._lookup_words)
    df = df.drop_duplicates()
    df['street'] = df['street'].apply(address_formatter)
    df['street'] = df.apply(lambda row: row.street['street'], axis=1)

    df['name'] = df['name'].apply(common.country_us._expand_rec_ctrs)

    # remove any row w/o all fields preset (because they failed to parse)
    df = df.dropna()
    # delete the old address blob to clean up & drop any remaining dupes
    df = df.drop_duplicates()

    return df
