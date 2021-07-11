import json
import requests


headers = requests.utils.default_headers()
headers['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0'

for idx in ['^GSPC', '^IXIC', '^DJI']:
    req = ''.join([
        "https://query2.finance.yahoo.com/v8/finance/chart/",
        idx,
        "?formatted=true&"
        "crumb=5dLyddyx4FN&",
        "lang=en-US&",
        "region=US&",
        "includeAdjustedClose=true&",
        "interval=1d&",
        "period1=1625443200&",
        "period2=1625875200&",
        "events=div|split&",
        "useYfid=true&",
        "corsDomain=finance.yahoo.com"
    ])
    res = requests.get(req, headers=headers)
    if res.status_code != 200:
        print(f'Something went wrong for {idx}: {res.text}')
        continue
    print(json.dumps(res.json(), indent=2))
