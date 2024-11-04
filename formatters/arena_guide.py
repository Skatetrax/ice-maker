from utils import common as common
import pandas as pd
import usaddress
import numpy as np


def address_formatter(x):
    # keep x for error reporting, use address for
    # processing/assembling data
    source = 'arena-guide'
    address = x

    try:
        address = usaddress.tag(address)
        address = address[0]
        street = address['StreetName'] + ' ' + address['StreetNamePostType']
        street = common.country_us._remove_punctuation(street)
        results = {
            'street': street,
            'city': address['PlaceName'],
            'state': address['StateName']
            }

    except usaddress.RepeatedLabelError as error:
        error = error.parsed_string
        message = f'error parsing "{x}" from "{source}": \n {error}'
        common.ice_maker_logging.fomatter_errors(message)
        results = {'street': np.nan, 'city': np.nan, 'state': np.nan}

    except (KeyError, TypeError) as error:
        error = error.args
        message = f'failed to parse {x} from "{source}", "{error}"'
        common.ice_maker_logging.fomatter_errors(message)
        results = {'street': np.nan, 'city': np.nan, 'state': np.nan}

    return results


def process_arena_guide():
    # setup US States mappings
    states = common.country_us.us_state_to_abbrev
    csv_data = '/tmp/ice-maker_raw_csv_arena-guide.csv'

    # Load the data of csv
    df = pd.read_csv(csv_data,
                    sep=';',
                    engine='python',
                    names=["Name", "Address", "street", "city", "state"])

    # remove any UTF-8 wierdness from WP scraping
    df['Name'] = df['Name'].apply(common.reset_utf8)

    # drop any obvious dupes, they're going to happen
    # and apply some normalization to the address section

    df['city'] = df['city'].apply(common.country_us._remove_punctuation)
    df['street'] = df['street'].apply(common.country_us._remove_punctuation)
    df = df.drop_duplicates()
    df['Address'] = df['Address'].apply(address_formatter)

    # convert street section to street addres column, and city, and state
    df['street'] = df.apply(lambda row: row.Address['street'], axis=1)
    df['city'] = df.apply(lambda row: row.Address['city'], axis=1)
    df['state'] = df.apply(lambda row: row.Address['state'], axis=1)

    # convert any full length state name to two letter abbreviation
    df['state'] = df['state'].map(lambda x: states.get(x, x))
    df['street'] = df['street'].map(common.country_us._lookup_words)

    df['Name'] = df['Name'].apply(common.country_us._expand_rec_ctrs)

    # remove any row w/o all fields preset (because they failed to parse)
    df = df.dropna()
    # delete the old address blob to clean up & drop any remaining dupes
    df = df.drop('Address', axis=1)
    df = df.drop_duplicates()

    return df
