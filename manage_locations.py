import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(name)s  %(levelname)s  %(message)s'
)

from pipeline.demoter import (
    demote_location, merge_locations, rename_location, search_locations,
)

parser = argparse.ArgumentParser(
    description='Manage ice-maker locations (demote, merge, search)'
)
sub = parser.add_subparsers(dest='command', required=True)

demote_p = sub.add_parser('demote', help='Change a location rink_status')
demote_p.add_argument('--name', type=str, help='Location name (exact or partial)')
demote_p.add_argument('--rink-id', type=str, help='Location UUID')
demote_p.add_argument(
    '--status', type=str, required=True,
    choices=['active', 'closed_permanently', 'seasonal', 'merged', 'disabled'],
    help='New rink_status value',
)

merge_p = sub.add_parser('merge', help='Merge one location into another')
merge_p.add_argument('--from-rink', type=str, required=True,
                     help='Rink ID to merge away (will be marked merged)')
merge_p.add_argument('--into-rink', type=str, required=True,
                     help='Rink ID to keep (receives sources and aliases)')

rename_p = sub.add_parser('rename', help='Rename a location (old name becomes alias)')
rename_p.add_argument('--name', type=str, help='Current location name (exact or partial)')
rename_p.add_argument('--rink-id', type=str, help='Location UUID')
rename_p.add_argument('--new-name', type=str, required=True,
                      help='New name for the location')

search_p = sub.add_parser('search', help='Search locations by name')
search_p.add_argument('query', type=str, help='Partial name to search for')
search_p.add_argument('--state', type=str, help='Filter by 2-letter state code')

args = parser.parse_args()

if args.command == 'demote':
    if not args.name and not args.rink_id:
        demote_p.error('Must provide --name or --rink-id')
    result = demote_location(
        name=args.name, rink_id=args.rink_id, status=args.status
    )
elif args.command == 'merge':
    result = merge_locations(from_id=args.from_rink, into_id=args.into_rink)
elif args.command == 'rename':
    if not args.name and not args.rink_id:
        rename_p.error('Must provide --name or --rink-id')
    result = rename_location(
        name=args.name, rink_id=args.rink_id, new_name=args.new_name
    )
elif args.command == 'search':
    search_locations(args.query, state=args.state)
    sys.exit(0)

if 'error' in result:
    print(f"\nERROR: {result['error']}")
    sys.exit(1)

print("\n=== Result ===")
for key, value in result.items():
    print(f"  {key}: {value}")
