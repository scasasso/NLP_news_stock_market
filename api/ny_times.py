import os
import time
import csv
import argparse
import shutil
import pandas as pd
from pynytimes import NYTAPI
import datetime
from tenacity import retry, stop_after_attempt, wait_fixed


API_KEY = '<INSERT YOUR KEY HERE>'
MAX_RANK = 10

QUERIES = {
    'Tesla': 'Tesla Motors Inc',
    'Ford': 'Ford Motor Co',
    'General Motors': 'General Motors'
}

INCLUDE_TAGS = [
    'Automobiles',
    'Electric and Hybrid Vehicles',
    'Driverless and Semiautonomous Vehicles'
]


def filter_by_keyword(a, keywords, max_rank=9999):
    akw = [kw['value'] for kw in a['keywords']]
    for i, kw in enumerate(akw):
        if kw in keywords and i <= max_rank:
            return True
    return False


@retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
def get_relevant_headlines(nyt, query, start, end, max_results=200, keywords=None):
    # Get datetime objects
    s, e = to_datetime(start), to_datetime(end)

    # Query articles
    articles = nyt.article_search(
        query=query,
        results=max_results,
        dates={
            "begin": s,
            "end": e
        },
        options={
            "sort": "oldest",
        }
    )

    # Filter
    if keywords is not None:
        articles = [a for a in articles if filter_by_keyword(a, keywords, max_rank=MAX_RANK)]

    # Prune
    if not len(articles) > 0:
        return pd.DataFrame()
    headlines = []
    for a in articles:
        failed = False
        d = {}
        try:
            d['publication'] = a['source']
        except:
            d['publication'] = 'The New York Times'
        try:
            d['abstract'] = a['abstract']
        except:
            d['abstract'] = ''
        try:
            d['section'] = a['section_name']
        except:
            d['section'] = 'Unknown'
        try:
            d['title'] = a['headline']['main']
            d['date'] = a['pub_date']
            d['matches'] = query
        except:
            failed = True
        for i in range(MAX_RANK):
            try:
                d[f'tag{i}'] = a['keywords'][i]['value']
            except:
                d[f'tag{i}'] = ''
        if not failed:
            headlines.append(d)
    return pd.DataFrame(headlines)


def to_datetime(dt):
    if isinstance(dt, pd.Timestamp):
        return dt.to_pydatetime()
    if isinstance(dt, str):
        return datetime.datetime.strptime(dt, '%Y-%m-%d')  # Must be in this format
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day)
    if isinstance(dt, datetime.datetime):
        return dt
    raise NotImplementedError(f'Cannot convert object of type {str(type(dt))}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', "--start_date", help="Start date in ISO format.", type=str, required=False, default='2011-01-01')
    parser.add_argument('-e', "--end_date", help="End date in ISO format.", type=str, required=False, default='2021-12-01')
    parser.add_argument('-o', "--output_file", help="Location of the output file.", type=str, required=False, default='ny_times.csv')
    args = parser.parse_args()

    # Create the session
    session = NYTAPI(API_KEY, parse_dates=True)

    # Directory keeping temporary files
    os.makedirs('./tmp/nytimes_chunks', exist_ok=True)

    # Split the requests in months to avoid breaking the API
    months = pd.date_range(args.start_date, args.end_date, freq='MS').tolist()
    months_start_end = [(months[i], months[i + 1] - pd.Timedelta(days=1)) for i in range(len(months) - 1)]

    # Loop over the queries (split by calendar months)
    output_dfs = []
    for query, kw in QUERIES.items():
        for (start, end) in months_start_end:
            time.sleep(3)
            start_str = start.strftime('%Y-%m-%d')
            end_str = end.strftime('%Y-%m-%d')
            print(f"Processing query {query}, start = {start_str}, end = {end_str}...")
            articles = get_relevant_headlines(session, query, start, end, keywords=[kw] + INCLUDE_TAGS)
            print(f'... found {len(articles)} news articles')
            out_name = '_'.join([query.replace(' ', ''), start_str, end_str])
            if len(articles) > 0:
                output_dfs.append(articles)
                articles.to_csv(f"./tmp/nytimes_chunks/{out_name}.csv", index=False)

    # Concatenate
    df = pd.concat(output_dfs)

    # Dedupe (multiple queries can fetch the same article)
    agg_dict = {'abstract': 'first', 'publication': 'first', 'section': 'first', 'date': 'first', 'matches': list}
    for i in range(MAX_RANK):
        agg_dict[f'tag{i}'] = 'first'
    df = df.groupby(['title']).agg(agg_dict).reset_index()

    # Write output
    df['n_matches'] = df['matches'].apply(len)
    df = df[['date', 'title', 'abstract', 'publication', 'section', 'matches', 'n_matches'] + [f'tag{i}' for i in
                                                                                               range(MAX_RANK)]]
    df.to_csv(args.output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

    # Cleanup
    shutil.rmtree('./tmp', ignore_errors=True)
