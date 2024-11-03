import re


# add some locale data


class country_us(object):
    '''
    Sk8erstuff.com search functions only works per state, so we need a list of
    every state in the US. There is an expectation that this will be used
    in other areas as well.
    '''
    states = [
        # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#States.
        "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA",
        "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
        "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
        "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
        "WV", "WY",
        # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Federal_district.
        "DC",
        # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Inhabited_territories.
        "AS", "GU", "MP", "PR", "VI",
    ]

    us_state_to_abbrev = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
        "District of Columbia": "DC",
        "American Samoa": "AS",
        "Guam": "GU",
        "Northern Mariana Islands": "MP",
        "Puerto Rico": "PR",
        "United States Minor Outlying Islands": "UM",
        "U.S. Virgin Islands": "VI",
        "AL": "AL",
        "AK": "AK",
        "AZ": "AZ",
        "AR": "AR",
        "CA": "CA",
        "CO": "CO",
        "CT": "CT",
        "DE": "DE",
        "FL": "FL",
        "GA": "GA",
        "HI": "HI",
        "ID": "ID",
        "IL": "IL",
        "IN": "IN",
        "IA": "IA",
        "KS": "KS",
        "KY": "KY",
        "LA": "LA",
        "ME": "ME",
        "MD": "MD",
        "MA": "MA",
        "MI": "MI",
        "MN": "MN",
        "MS": "MS",
        "MO": "MO",
        "MT": "MT",
        "NE": "NE",
        "NV": "NV",
        "NH": "NH",
        "NJ": "NJ",
        "NM": "NM",
        "NY": "NY",
        "NC": "NC",
        "ND": "ND",
        "OH": "OH",
        "OK": "OK",
        "OR": "OR",
        "PA": "PA",
        "RI": "RI",
        "SC": "SC",
        "SD": "SD",
        "TN": "TN",
        "TX": "TX",
        "UT": "UT",
        "VT": "VT",
        "VA": "VA",
        "WA": "WA",
        "WV": "WV",
        "WI": "WI",
        "WY": "WY",
        "DC": "DC",
        "AS": "AS",
        "GU": "GU",
        "MP": "MP",
        "PR": "PR",
        "UM": "UM",
        "VI": "VI",
        }

    st_abbr = {
        'APT': 'APARTMENT',
        'APTS': 'APARTMENTS',
        'AVE': 'AVENUE',
        'BLVD': 'BOULEVARD',
        'BR': 'BRIDGE',
        'CIR': 'CIRCLE',
        'CT': 'COURT',
        'DR': 'DRIVE',
        'HWY': 'HIGHWAY',
        'HW': 'HIGHWAY',
        'LK': 'LAKE',
        'LN': 'LANE',
        'RD': 'ROAD',
        'MT': 'MOUNT',
        'MTN': 'MOUNTAIN',
        'PKWY': 'PARKWAY',
        'PL': 'PLACE',
        'RTE': 'ROUTE',
        'SQ': 'SQUARE',
        'ST': 'STREET',
        'STE': 'SUITE',
        'TPKE': 'TURNPIKE',
        'TR': 'TRAIL'
        }

    def _lookup_words(input_text):
        abbr_dict = country_us.st_abbr

        try:
            words = input_text.upper().split()
            new_words = []
            for word in words:
                if word.upper() in abbr_dict:
                    word = abbr_dict[word.upper()]
                new_words.append(word)
            new_text = " ".join(new_words)
        except:
            new_text = input_text

        return new_text

    def _remove_punctuation(input_text):
        try:
            output_text = re.sub(r'[^\w\s]', '', input_text)
        except:
            output_text = input_text
        return output_text


def reset_utf8(input_text):
    try:
        output_text = input_text.encode('ISO-8859-1').decode('utf8')
    except:
        output_text = input_text
    return output_text
