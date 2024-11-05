from formatters import sk8stuff as sk8stuff
from formatters import arena_guide as arena_guide
from formatters import learntoskate

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


def generate_learntoskate():

    report = '/tmp/ice-maker_formatted_lts.csv'

    df = pd.DataFrame(learntoskate.process_lts())
    df = df.assign(Date=datetime.now())
    df = df.assign(Source='LTS')
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
    try:
        generate_sk8stuff()
    except FileNotFoundError:
        print('No RAW files found, did you run "generate_raw_csvs.py"?')

elif args['source'] == 'arena_guide':
    try:
        generate_arena_guide()
    except FileNotFoundError:
        print('No RAW files found, did you run "generate_raw_csvs.py"?')

elif args['source'] == 'lts':
    try:
        generate_learntoskate()
    except FileNotFoundError:
        print('No RAW files found, did you run "generate_raw_csvs.py"?')

elif args['source'] == 'all':
    report = '/tmp/ice-maker_formatted_all.csv'
    df0 = pd.DataFrame()
    try:
        df1 = generate_sk8stuff()
        df2 = generate_arena_guide()
        df3 = generate_learntoskate()

        df0 = pd.concat([df1, df2, df3], axis=0)

        print("Generating master report to", report)
        df0.to_csv(report, sep=';', encoding='utf-8', index=False, header=False)
    except FileNotFoundError:
        print('No RAW files found, did you run "generate_raw_csvs.py"?')

else:
    print('No Known Source Specified')
