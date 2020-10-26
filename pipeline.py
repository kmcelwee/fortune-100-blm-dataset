import json
import pandas as pd
import os
from datetime import datetime
import pytz


def combine_json():
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
                'created_at': x['created_at'],
                'id': x['id'],
                'full_text': x['full_text'].replace('\r', ' '),
                'hashtags': ';'.join([h['text'] for h in x['entities']['hashtags']]),
                'corporation': handle_map[filename.split('.')[0]]
            } for x in company_json if ((x['in_reply_to_user_id'] is None) and (not x['full_text'].startswith('@')))
        ])

        dfs.append(df_t)

    df = pd.concat(dfs)

    cols = ['Datetime', 'ID', 'Text', 'Hashtags', 'Corporation']
    df.columns = cols

    df['dt'] = pd.to_datetime(df['Datetime'])
    END = datetime(2020, 7, 26, tzinfo=pytz.utc)
    df_clipped = df[df['dt'] <= END]

    df_clipped[cols].to_csv('tmp/fortune-100-trimmed.csv', index=False)

def link_corporate_and_blm_tweets():
    """Extend columns on both the Fortune 100 and BLM CSVs"""

    # Load all the CSVs we'll be linking
    df_b = pd.read_csv('data/blm-tweets-categorized.csv')
    df_t = pd.read_csv('tmp/fortune-100-trimmed.csv')
    df_i = pd.read_csv('data/fortune-100.csv')

    ####
    # Data was collected through the UI, which does not offer access to the 
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
    def is_rj(r):
        # If before May 25, it was not categorized, so mark as null, otherwise T/F
        if r['dt'] < datetime(2020, 5, 25, 0, 0, 0):
            return None
        else:
            return r['ID'] in df_b['ID'].tolist()

    df_t['dt'] = df_t['Datetime'].astype('datetime64')
    df_t['Racial Justice'] = df_t.apply(is_rj, axis=1)
    assert df_t['Racial Justice'].sum() == df_b.shape[0]


    # Collect only necessary columns, and output to final CSV.
    cols = ['ID', 'Corporation', 'Text', 'Datetime', 'Racial Justice', 'Hashtags']
    df_t[cols].to_csv('data/fortune-100-tweets.csv', index=False)


    ####
    # Extend BLM Tweets with tweet info
    # HACK: There's a merge / join command for this, but this'll have to do for now.
    df_t.index = df_t['ID']
    df_b.index = df_b['ID']
    df_b['Text'] = [df_t.loc[i]['Text'] for i, r in df_b.iterrows()]
    df_b['Hashtags'] = [df_t.loc[i]['Hashtags'] for i, r in df_b.iterrows()]
    df_b['Datetime'] = [df_t.loc[i]['Datetime'] for i, r in df_b.iterrows()]
    
    # Collect only necessary columns, and output to final CSV.
    cols = ['Corporation', 'Handle', 'Text', 'Datetime', 'Hashtags', 'BLM', 'Juneteenth', 'Money', 'Formal Statement']
    df_b[cols].to_csv('data/blm-tweets.csv')


def run_pipeline():
    combine_json()
    link_corporate_and_blm_tweets()

if __name__ == '__main__':
    run_pipeline()