import os
import requests
import string

# TODO:
# Iterate through the alphabet letters
# Get https://etfdb.com/alpha/A/, ... https://etfdb.com/alpha/Z/
# To get the next page do:
# https://etfdb.com/alpha/B/#etfs&sort_name=symbol&sort_order=asc&page=2
# Get each ETF:
# https://etfdb.com/etf/AAAU/#etf-ticker-profile
# etfdb only has top 15 holdings, can I get full list from yhoo or somewhere else
# zacks seems to have a list of holdings for each ETF: https://www.zacks.com/funds/etf/ACES/holding


for letter in list(string.ascii_uppercase):
    etf_url = f'https://etfdb.com/alpha/{letter}/'


# begin - parse the holdings info for each fund
filename = os.path.join(os.getenv('HOME'), 'want.html')
with open(filename, 'r') as f:
    text = f.read()
lines = text.split('\n')
holdings_line = ''
for line in lines:
    if line.startswith('etf_holdings.formatted_data'):
        holdings_line = line
holdings_tokens = holdings_line[34: -5].split(' ] ,  [ ')

hold_list = []

for holding_row in holdings_tokens:
    holding_tokens = holding_row.split(', ')
    ticker_token = holding_tokens[1]
    ticker_index = ticker_token.find('rel=')
    if ticker_index == -1:
        continue
    ticker_tokens = ticker_token[ticker_index:].split('\\"')
    if len(ticker_tokens) >= 2:
        ticker = ticker_tokens[1]
        hold_list.append(ticker)
    
print(f'ETF WANT has {len(hold_list)} equity holdings: {hold_list}')
# end - parse the holdings info for each fund
