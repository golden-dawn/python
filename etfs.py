import json
import os
import requests
import string
import stxdb
import sys
import time

# TODO:
# Iterate through the alphabet letters
# Get https://etfdb.com/alpha/A/, ... https://etfdb.com/alpha/Z/
# To get the next page do:
# https://etfdb.com/alpha/B/#etfs&sort_name=symbol&sort_order=asc&page=2
# Get each ETF:
# https://etfdb.com/etf/AAAU/#etf-ticker-profile
# etfdb only has top 15 holdings, can I get full list from yhoo or somewhere else
# zacks seems to have a list of holdings for each ETF: https://www.zacks.com/funds/etf/ACES/holding
# https://etfdb.com/data_set/?tm=40278&cond={"by_alphabetical":"A"}&no_null_sort=true&count_by_id=&limit=25,25&sort=symbol&order=asc&offset=50

# for letter in list(string.ascii_uppercase):
#     etf_url = f'https://etfdb.com/alpha/{letter}/'

# filename = os.path.join(os.getenv('HOME'), 'etfs_a.json')
# with open(filename) as f:
#     etfs_json = json.load(f)

# etf_rows = etfs_json.get('rows', [])
# if not etf_rows:
#     print('No ETFs found')

# etf_ix, processed_etfs = 0, 0
# for etf in etf_rows:
#     etf_ix += 1
#     print(f'{json.dumps(etf, indent=2)}')
#     etf_symbol_raw = etf.get('symbol')
#     if not etf_symbol_raw:
#         print(f'ETF {etf_ix} missing symbol')
#         continue
#     symbol_tokens = etf_symbol_raw.split('\u003e')
#     etf_symbol = symbol_tokens[2][:-6]
#     etf_name_raw = etf.get('name')
#     if not etf_name_raw:
#         print(f'ETF {etf_ix} missing name')
#         continue
#     name_tokens = etf_name_raw.split('\u003e')
#     if len(name_tokens) < 4:
#         print(f'ETF {etf_ix} failed to parse name text (not enough tokens)')
#         continue
#     name_raw = name_tokens[3]
#     iix = name_raw.find('<span')
#     if iix == -1:
#         print(f'ETF {etf_ix} failed to parse name text (no span)')
#         continue
#     etf_name = name_raw[:iix].strip()
#     etf_category = etf.get('etf_category', 'N/A')
#     print(f'ETF ticker {etf_symbol}, name {etf_name}, category {etf_category}')
#     processed_etfs += 1
# print(f'Processed {processed_etfs} out of {len(etf_rows)} ETFs')

# filename = os.path.join(os.getenv('HOME'), 'etf_b.txt')
# with open(filename, 'r') as f:
#     text = f.read()
# lines = text.split('\n')
# start_ix = 1000000
# end_ix = -1
# for i, line in enumerate(lines):
#     stripped_line = line.strip()
#     if stripped_line == '<tbody>':
#         start_ix = i
#     if stripped_line == '</tbody>':
#         end_ix = i
#     if i <= start_ix:
#         continue
#     if i <= end_ix:
#         break
#     if stripped_line in ['<tr>', '</tr>']:
#         continue
#     line_tokens = stripped_line.split('</td> <td class="" data-th=')
#     print(f'{line_tokens}')
#     tkns0 = line_tokens[0].split('>')
#     etf_ticker = tkns0[2][:-3] if len(tkns0) > 2 else ''
#     tkns1 = line_tokens[1].split('>')
#     etf_name = tkns1[2][:-3] if len(tkns1) > 2 else ''
#     tkns2 = line_tokens[2].split('>')
#     etf_category = tkns2[2][:-3] if len(tkns2) > 2 else ''
#     print(f'etf_ticker = {etf_ticker}, etf_name = {etf_name} etf_category = {etf_category}')


# etf_rows = etfs_json.get('rows', [])
# if not etf_rows:
#     print('No ETFs found')

# etf_ix, processed_etfs = 0, 0
# for etf in etf_rows:
#     etf_ix += 1
#     print(f'{json.dumps(etf, indent=2)}')
#     etf_symbol_raw = etf.get('symbol')
#     if not etf_symbol_raw:
#         print(f'ETF {etf_ix} missing symbol')
#         continue
#     iix1 = etf_symbol_raw.find('href=\"/etf/')
#     iix2 = etf_symbol_raw.find('/\"')
#     etf_symbol = etf_symbol_raw[iix1 + 12: iix2]
#     etf_name_raw = etf.get('name')
#     if not etf_name_raw:
#         print(f'ETF {etf_ix} missing name')
#         continue
#     name_tokens = etf_name_raw.split('\u003e')
#     if len(name_tokens) < 4:
#         print(f'ETF {etf_ix} failed to parse name text (not enough tokens)')
#         continue
#     name_raw = name_tokens[3]
#     iix = name_raw.find('<span')
#     if iix == -1:
#         print(f'ETF {etf_ix} failed to parse name text (no span)')
#         continue
#     etf_name = name_raw[:iix].strip()
#     etf_category = etf.get('etf_category', 'N/A')
#     print(f'ETF ticker {etf_symbol}, name {etf_name}, category {etf_category}')
#     processed_etfs += 1
# print(f'Processed {processed_etfs} out of {len(etf_rows)} ETFs')

# filename = os.path.join(os.getenv('HOME'), 'etf_b.txt')
# with open(filename, 'r') as f:
#     text = f.read()
# lines = text.split('\n')
# start_ix = 1000000
# end_ix = -1
# for i, line in enumerate(lines):
#     stripped_line = line.strip()
#     if stripped_line == '<tbody>':
#         start_ix = i
#     if stripped_line == '</tbody>':
#         end_ix = i
#     if i <= start_ix:
#         continue
#     if i <= end_ix:
#         break
#     if stripped_line in ['<tr>', '</tr>']:
#         continue
#     line_tokens = stripped_line.split('</td> <td class="" data-th=')
#     print(f'{line_tokens}')
#     tkns0 = line_tokens[0].split('>')
#     etf_ticker = tkns0[2][:-3] if len(tkns0) > 2 else ''
#     tkns1 = line_tokens[1].split('>')
#     etf_name = tkns1[2][:-3] if len(tkns1) > 2 else ''
#     tkns2 = line_tokens[2].split('>')
#     etf_category = tkns2[2][:-3] if len(tkns2) > 2 else ''
#     print(f'etf_ticker = {etf_ticker}, etf_name = {etf_name} etf_category = {etf_category}')

filename = os.path.join(os.getenv('HOME'), 'reports', 'etf_list.txt')
with open(filename, 'r') as f:
    text = f.read()
lines = text.split('\n')
for line in lines:
    tokens = line.split('\t')
    if len(tokens) > 4:       
        etf_ticker = tokens[0]
        etf_name = tokens[1].replace("'", '')
        etf_category = tokens[2]
        stxdb.db_write_cmd(
            f"INSERT INTO etfs VALUES('{etf_ticker}', '{etf_name}', "
            f"'{etf_category}') ON CONFLICT (ticker) DO NOTHING"
        )


# begin - parse the holdings info for each fund

etf_list = stxdb.db_read_cmd('select ticker from etfs')
num_etfs = len(etf_list)
print(f'Getting data for {num_etfs} ETFs')
headers = requests.utils.default_headers()
headers['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) '\
    'Gecko/20100101 Firefox/88.0'
etf_name = 'XLE'
for i, etf_record in enumerate(etf_list):
    etf_name = etf_record[0]
    req = requests.get(f"https://www.zacks.com/funds/etf/{etf_name}/holding",
                       headers=headers)
    if req.status_code != 200:
        print(f'Request for ETF {etf_name} failed with status '
              f'{req.status_code}, error: {req.text}')
        continue
    lines = req.text.split('\n')
    # print(f'The request response has {len(lines)} lines')
    holdings_line = ''
    for line in lines:
        if line.startswith('etf_holdings.formatted_data'):
            holdings_line = line
    if not holdings_line:
        print(f'No holdings line found for {etf_name}, skipping...')
        continue
    holdings_tokens = holdings_line[34: -5].split(' ] ,  [ ')
    # print(f'There are {len(holdings_tokens)} holdings tokens')
    hold_list = []
    for holding_row in holdings_tokens:
        # print(f'holding_row = {holding_row}')
        holding_tokens = holding_row.split(', ')
        # print(f'There are {len(holding_tokens)} holding tokens')
        if len(holding_tokens) < 2:
            continue
        ticker_token = holding_tokens[1]
        # print(f'Ticker token = {ticker_token}')
        ticker_index = ticker_token.find('rel=')
        # print(f'Ticker index = {ticker_index}')
        if ticker_index == -1:
            continue
        ticker_tokens = ticker_token[ticker_index:].split('\\"')
        # print(f'There are {len(ticker_tokens)} ticker tokens')
        if len(ticker_tokens) >= 2:
            ticker = ticker_tokens[1]
            hold_list.append(ticker)
            stxdb.db_write_cmd(
                f"INSERT INTO stk_etfs VALUES('{ticker}', '{etf_name}') "
                f"ON CONFLICT(stk, etf) DO NOTHING"
            )
    print(f'ETF {etf_name} has {len(hold_list)} equity holdings: {hold_list}')
    if i > 0 and (i % 100 == 0 or i == num_etfs - 1):
        print(f'Got data for {i} out of {num_etfs} ETFS')
    time.sleep(2)
# end - parse the holdings info for each fund
