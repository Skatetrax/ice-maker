from utils import common as common
import pandas as pd
import usaddress
import re


def address_formatter(x):

    # remove unneeded, inconsitent country and zip data
    x = x.removesuffix("United States of America").strip()
    x = x.removesuffix("United States").strip()
    x = x.removesuffix("USA").strip()
    x = re.sub(r"\s?\d+$", "", x).strip()
    x = x.rstrip(',')

    # keep x for error reporting, use address for
    # processing/assembling data
    address = x

    try:
        address = usaddress.tag(address)
        address = address[0]

        results = {
            'street': address['StreetName'] + ' ' + address['StreetNamePostType'],
            'city': address['PlaceName'],
            'state': address['StateName']
            }

    except:
        print("failed to parse:", x)
        results = {'street': '', 'city': '', 'state': ''}

    return results


# setup US States mappings
states = common.country_us.us_state_to_abbrev

# csv_data = 'ice_maker_AG_seed_data.csv'
csv_data = 'ag.csv'

# Load the data of csv
df = pd.read_csv(csv_data,
                 sep=';',
                 engine='python',
                 names=["Name", "Address", "street", "city", "state"])

# drop any obvious dupes, they're going to happen
# and apply some normalization to the address section
df = df.drop_duplicates()
df['Address'] = df['Address'].apply(address_formatter)

# convert street section to street addres column, and city, and state
df['street'] = df.apply(lambda row: row.Address['street'], axis=1)
df['city'] = df.apply(lambda row: row.Address['city'], axis=1)
df['state'] = df.apply(lambda row: row.Address['state'], axis=1)

# convert any full length state name to two letter abbreviation
df['state'] = df['state'].map(lambda x: states.get(x, x))

# delete the old address blob to save space and drop any remaining dupes
df = df.drop('Address', axis=1)
df = df.drop_duplicates()


print(df)
out_file_name = '/tmp/icemaker_arena-guide.csv'
print('Saving dataframe to:', out_file_name)
df.to_csv(out_file_name, sep=';', encoding='utf-8', index=False, header=True)

#stats = df.groupby(['state']).size()
#print(stats)
