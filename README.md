# ice-maker

git pull; cd ice-maker
pipenv shell
pipenv install
python generate_raw_csvs.py --sources all
python generate_rink_list.py --sources all

Google Sheets data available via https://docs.google.com/spreadsheets/d/1r_LrJMJmPXWjf77FobyOX0sXgtyYL74mkP9GL2J97rc