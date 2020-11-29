import os
import pytz
import json
from datetime import datetime

import pandas as pd

def validate_csvs():
    # Ensure that all CSVs are loadable
    df_b = pd.read_csv('data/blm-tweets.csv')
    df_t = pd.read_csv('data/fortune-100-tweets.csv', parse_dates=['Datetime'])
    df_i = pd.read_csv('data/fortune-100.csv')
    df_m = pd.read_csv('data/blm-tweets-categorized.csv')

    # Ensure all BLM tweet ids are in the Fortune 100 tweet CSV
    assert all([i in df_t['ID'].tolist() for i in df_b['ID']])

    # Ensure all Racial Justice categorizations before May 25 are null
    before_tweets = df_t[df_t['Datetime'] < datetime(2020, 5, 25, 0, 0, 0)]
    after_tweets = df_t[df_t['Datetime'] >= datetime(2020, 7, 26, 0, 0, 0)]
    assert before_tweets.shape[0] == 0
    assert after_tweets.shape[0] == 0

    # Ensure manual CSV is not malformed
    assert len(df_m.columns) == 7
    bool_cols = ['BLM', 'Juneteenth', 'Money', 'Formal Statement']
    assert all(df_m[col].dtype == bool for col in bool_cols)

def validate_json():
    """Ensure that JSONs are formatted properly"""
    for filename in os.listdir('data/fortune-100-json'):
        if filename.endswith('.json'):
            with open(f'data/fortune-100-json/{filename}') as f:
                company_json = json.load(f)

                # Ensure no tweets were truncated
                assert not any([tweet['truncated'] for tweet in company_json])
                # Ensure no IDs were rounded
                assert all([str(tweet['id']) == tweet['id_str'] for tweet in company_json])


def run_tests():
    validate_csvs()
    validate_json()


if __name__ == '__main__':
    run_tests()