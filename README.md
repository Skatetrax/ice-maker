# ice-maker

Daily:
Read web source containing list of ice rinks
turn each rink entry into a row in database table

Hourly:
Read table rows, validate entry name and address
Update row with google information when discrepencies exist

Arena-Guide Parser: Only returns addresses of first page, use this as a way to seed records to be investigated by google maps API. Help wanted on fixing pagination.

Sk8erStuff Parser: Returns a list of dictionaries with name, address, phone number if available. Searchable via abbreviated state.

Google Sheets data available via https://docs.google.com/spreadsheets/d/1r_LrJMJmPXWjf77FobyOX0sXgtyYL74mkP9GL2J97rc