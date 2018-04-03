import requests

req = 'https://stooq.com/q/d/l?s={0:s}&d1={1:s}&d2={2:s}'
s_date = '2018-03-01'
e_date = '2018-04-02'
sd = s_date.replace('-', '')
ed = e_date.replace('-', '')

with open('symbols.txt', 'r') as f:
    lines = f.readlines()

for line in lines:
    ticker = line.strip()
    r = requests.get(req.format(ticker, sd, ed))
    print('{0:s}\n{1:s}'.format(ticker, r.text))
