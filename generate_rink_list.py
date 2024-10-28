from formatters import sk8stuff as sk8stuff
from formatters import arena_guide as arena_guide
from datetime import datetime
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", help="show some useful help text")
args = vars(parser.parse_args())


def generate_arena_guide():

    report = '/tmp/ice-maker_formatted_arena-guide.csv'

    df = pd.DataFrame(arena_guide.process_arena_guide())
    df = df.assign(Date=datetime.now())
    df = df.assign(Source='Arena-Guide')
    print("Generating report to", report)
    df.to_csv(report, sep=';', encoding='utf-8', index=False, header=False)

    return df


def generate_sk8stuff():

    report = '/tmp/ice-maker_formatted_sk8stuff.csv'

    df = pd.DataFrame(sk8stuff.process_sk8stuff())
    df = df.assign(Date=datetime.now())
    df = df.assign(Source='Sk8Stuff')

    print("Generating report to", report)
    df.to_csv(report, sep=';', encoding='utf-8', index=False, header=False)

    return df


if args['source'] == 'sk8stuff':
    generate_sk8stuff()

elif args['source'] == 'arena_guide':
    generate_arena_guide()

elif args['source'] == 'all':
    report = '/tmp/ice-maker_formatted_all.csv'
    df0 = pd.DataFrame()
    df1 = generate_sk8stuff()
    df2 = generate_arena_guide()

    df0 = pd.concat([df1, df2], axis=0)

    print("Generating master report to", report)
    df0.to_csv(report, sep=';', encoding='utf-8', index=False, header=False)

else:
    print('No Known Source Specified')

#print(generate_sk8stuff())
