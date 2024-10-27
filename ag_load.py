from utils import common as common
import pandas as pd
import usaddress


def address_formatter(x):
    try:
        x = usaddress.tag(x)
        x = x[0]
        results = {
            'street': x['StreetName'] + ' ' + x['StreetNamePostType'],
            'city': x['PlaceName'],
            'state': x['StateName']
            }
    except:
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


df['Address'] = df['Address'].apply(address_formatter)
df['street'] = df.apply(lambda row: row.Address['street'], axis=1)
df['street'] = df['street'].str.replace('.', '', regex=False)
df['city'] = df.apply(lambda row: row.Address['city'], axis=1)
df['state'] = df.apply(lambda row: row.Address['state'], axis=1)
df['state'] = df['state'].map(lambda x: states.get(x, x))
df = df.drop('Address', axis=1)
df = df.drop_duplicates()


out_file_name = '/tmp/icemaker_arena-guide.csv'
#print('Saving dataframe to:', out_file_name)
#df.to_csv(out_file_name, sep=';', encoding='utf-8', index=False, header=True)

#stats = df.groupby(['state']).size()
#print(stats)
