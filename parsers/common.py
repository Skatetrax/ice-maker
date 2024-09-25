# add some locale data


class country_us(object):
    '''
    Sk8er-stuff search functions only works per state, so we need a list of
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
