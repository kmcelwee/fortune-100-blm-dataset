import json
import pandas as pd
import os
import click
import wget

from os.path import join as pjoin
from datetime import datetime

def combine_json():
    def parse_datetime(s):
        return datetime.strptime(s, "%a %b %d %H:%M:%S +0000 %Y")

    def should_include_tweet(tweet_json):
        # Given the raw scrape data, what tweets should be included in analysis?
        BEGIN = datetime(2020, 5, 25, 0, 0, 0)
        END = datetime(2020, 7, 26, 0, 0, 0)

        return (
            (tweet_json['in_reply_to_user_id'] is None) and
            (not tweet_json['full_text'].startswith('@')) and
            (BEGIN <= parse_datetime(tweet_json['created_at']) < END)
        )

    # Make a mapper from handle to corporation name
    df_i = pd.read_csv('data/fortune-100.csv')
    handle_map = { r['Handle']: r['Corporation'] for i, r in df_i.iterrows() }

    # Trim JSON data and combine into one CSV
    dfs = []
    for filename in [x for x in os.listdir('data/fortune-100-json') if x.endswith('.json')]:
        with open('data/fortune-100-json/' + filename) as f:
            company_json = json.load(f)
        df_t = pd.DataFrame([
            {
                'Datetime': parse_datetime(x['created_at']),
                'ID': x['id'],
                'Text': x['full_text'].replace('\r', ' '),
                'Hashtags': ';'.join([h['text'] for h in x['entities']['hashtags']]),
                'Corporation': handle_map[filename.split('.')[0]]
            } for x in company_json if should_include_tweet(x)
        ])

        dfs.append(df_t)

    df = pd.concat(dfs)
    df.to_csv('tmp/fortune-100-trimmed.csv', index=False)

def link_corporate_and_blm_tweets():
    """Extend columns on both the Fortune 100 and BLM CSVs"""

    # Load all the CSVs we'll be linking
    df_b = pd.read_csv('data/blm-tweets-categorized.csv')
    df_t = pd.read_csv('tmp/fortune-100-trimmed.csv')
    df_i = pd.read_csv('data/fortune-100.csv')

    ####
    # Data was collected through the UI, which does not always offer access to the 
    #   retweet ID. Here we match BLM tweet which has original ID with the data
    #   collected through the API, which contains the retweet ID and the
    #   original ID.
    # HACK: This can be more elegant than 30 lines of code.
    blm_ids = df_b['ID'].tolist()
    retweet = []
    for filename in os.listdir('data/fortune-100-json'):
        if not filename.startswith('.'):
            with open('data/fortune-100-json/' + filename) as f:
                company_json = json.load(f)
            for tweet in company_json:
                if 'retweeted_status' in tweet:
                    if tweet['retweeted_status']['id'] in blm_ids:
                        retweet.append({
                            'corporation': filename.split('.json')[0],
                            'retweet_id': tweet['id'],
                            'og_id': tweet['retweeted_status']['id']
                        })

    # Ensure that the number of retweet IDs, calculated through two different 
    #   methods are equal.
    blm_retweet_ids = df_b[~df_b['Handle'].isin(df_i['Handle'])]['ID'].tolist()
    assert len(retweet) == len(blm_retweet_ids)

    def get_corporation(blm_id):
        # match a corporation with a retweet ID
        if blm_id not in df_t['ID'].tolist():
            in_retweet = [x for x in retweet if blm_id == x['og_id']]
            if len(in_retweet) == 1:
                blm_id = in_retweet[0]['retweet_id']
            else:
                raise StandardError(f"There's not a one-to-one match with ID: {blm_id}")
        return df_t[df_t['ID'] == blm_id]['Corporation'].iloc[0]

    df_b['Corporation'] = df_b['ID'].apply(get_corporation)


    ####
    # Swap original IDs in the BLM CSV for the retweet IDs
    id_mapper = {r['og_id']: r['retweet_id'] for r in retweet}
    df_b['ID2'] = df_b['ID']
    df_b['ID'] = df_b['ID2'].apply(lambda x: id_mapper.get(x, x))
    assert all([x in df_t['ID'].tolist() for x in df_b['ID'].tolist()])


    ####
    # Add a "Racial Justice" column to mark IDs in the BLM tweets dataset
    df_t['dt'] = df_t['Datetime'].astype('datetime64')
    df_t['Racial Justice'] = df_t.apply(lambda r: r['ID'] in df_b['ID'].tolist(), axis=1)
    assert df_t['Racial Justice'].sum() == df_b.shape[0]

    ####
    # Extend Fortune 100 Tweets with BLM info
    df_t.index = df_t['ID']
    df_b.index = df_b['ID']
    df_t['BLM'] = [df_b.loc[i]['BLM'] if r['Racial Justice'] else None for i, r in df_t.iterrows()]
    df_t['Juneteenth'] = [df_b.loc[i]['Juneteenth'] if r['Racial Justice'] else None for i, r in df_t.iterrows()]
    df_t['Money'] = [df_b.loc[i]['Money'] if r['Racial Justice'] else None for i, r in df_t.iterrows()]

    # Collect only necessary columns, and output to final CSV.
    cols = ['Corporation', 'Text', 'Datetime', 'Hashtags', 'Racial Justice', 'BLM', 'Juneteenth', 'Money']
    df_t[cols].to_csv('data/fortune-100-tweets.csv')


def download_media():
    """Collect all images from Racial Justice tweets"""
    DATA_DIR = 'data'
    RAW_DATA_DIR = 'data/fortune-100-json'
    OUTPUT_DIR = 'data/rj-imgs'

    # Get the IDs of Racial Justice tweets
    df_t = pd.read_csv(pjoin(DATA_DIR, 'fortune-100-tweets.csv'))
    df_b = df_t[df_t['Racial Justice']]
    racial_justice_ids = set(df_b['ID'].tolist())

    # Collect RJ tweets that contain media
    rj_tweets = []
    for filename in os.listdir(RAW_DATA_DIR):
        with open(pjoin(RAW_DATA_DIR, filename)) as f:
            company_json = json.load(f)
        for tweet in company_json:
            if tweet['id'] in racial_justice_ids:
                rj_tweets.append(tweet)
    media_tweets = [x for x in rj_tweets if 'media' in x['entities']]

    # Download the image from each of the RJ tweets that contain media
    for tweet in media_tweets:
        for i, media in enumerate(tweet['entities']['media']):
            url = media['media_url']
            extension = url.split('.')[-1]
            assert extension in ['jpg', 'png']
            output_file = f"{tweet['user']['name']}-{tweet['id']}-{i}.{extension}"
            wget.download(url, out=pjoin(OUTPUT_DIR, output_file))


@click.command()
@click.option('--download', is_flag=True, help="Download images from tweets")
def run_pipeline(download):
    combine_json()
    link_corporate_and_blm_tweets()
    if download:
        download_media()


if __name__ == '__main__':
    run_pipeline()