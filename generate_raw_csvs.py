import parsers.sk8stuff as sk8stuff
import parsers.arena_guide as arena_guide
import parsers.learntoskate as lts
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--source", help="show some useful help text")
args = vars(parser.parse_args())


def generate_lts():
    print('Generating RAW CSV for LTS...')

    path = '/tmp/ice-maker_raw_csv_lts.csv'
    lts.lts_csv(path)

    print('Complete! CSV located at', path)


def generate_sk8stuff():
    print('Generating RAW CSV for Sk8Stuff...')

    path = '/tmp/ice-maker_raw_csv_sk8stuff.csv'
    sk8stuff.sk8stuff_csv(path)

    print('Complete! CSV located at', path)


def generate_arena_guide():
    print('Generating RAW CSV for Arena-Guide...')

    path = '/tmp/ice-maker_raw_csv_arena-guide.csv'
    arena_guide.arena_guide_csv(path)

    print('Complete! CSV located at', path)


if args['source'] == 'sk8stuff':
    generate_sk8stuff()
elif args['source'] == 'arena_guide':
    generate_arena_guide()
elif args['source'] == 'lts':
    generate_lts()
elif args['source'] == 'all':
    generate_sk8stuff()
    generate_arena_guide()
    generate_lts()
else:
    print('No Known Source Specified')
