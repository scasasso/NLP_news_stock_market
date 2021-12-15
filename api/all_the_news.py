import re
import pandas as pd
import argparse


def get_matches(hl):
    l1 = [r'\btesla\b', r'\bgeneral motors\b', r'\bford\b']
    l2 = [r'\bGM\b', r'\bTSLA\b']
    n_ford = [r'Harrison', 'Christine Blasey', 'Blasey', 'Kavanaugh']
    try:
        hll = hl.lower()
        m_l1 = re.findall(r"(?=("+'|'.join(l1)+r"))", hll)
        m_l2 = re.findall(r"(?=("+'|'.join(l2)+r"))", hl)
        mn_ford = re.findall(r"(?=("+'|'.join(n_ford)+r"))", hll, re.IGNORECASE)
        out = list(set(m_l1 + m_l2))
        if len(mn_ford) > 0:
            out.remove('ford')
        return out
    except Exception:
        return []


def process(data):
    data['matches'] = data['title'].apply(get_matches)
    data['n_matches'] = data['matches'].apply(len)
    return data[data['n_matches'] > 0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', "--input_file", help="Location of the input csv file.", type=str, required=True)
    parser.add_argument('-o', "--output_file", help="Location of the output file.", type=str, required=False, default='all_the_news.csv')
    parser.add_argument('-c', "--chunk_size", help="Chunk size for processing", type=int, required=False, default=10 **4)
    args = parser.parse_args()

    i = 0
    output_dfs = []
    for chunk in pd.read_csv(args.input_file, chunksize=args.chunk_size, usecols=[2, 7, 11]):
        if i % 10 == 0:
            print(f'Processing chunk {i}')
        chunk_p = process(chunk)
        if len(chunk_p) > 0:
            output_dfs.append(chunk_p)
        i += 1

    # Concatenate
    df = pd.concat(output_dfs)
    df['date'] = pd.to_datetime(df['date'])
    print(f'Output file contains {len(df)} entries.')

    # Write to output
    df.to_csv(args.output_file, index=False)
    print(f'Output file written: {args.output_file}')
