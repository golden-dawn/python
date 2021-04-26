import os
import requests

# TODO:
# Iterate through the alphabet letters
# Get https://etfdb.com/alpha/A/, ... https://etfdb.com/alpha/Z/
# Get each ETF:
# https://etfdb.com/etf/AAAU/#etf-ticker-profile
# etfdb only has top 15 holdings, can I get full list from yhoo or somewhere else
# zacks seems to have a list of holdings for each ETF: https://www.zacks.com/funds/etf/ACES/holding

filename = os.path.join(os.getenv('HOME'), 'want.html')
with open(filename, 'r') as f:
    text = f.read()
lines = text.split('\n')
holdings_line = ''
for line in lines:
    if line.startswith('etf_holdings.formatted_data'):
        holdings_line = line
holdings_tokens = holdings_line[34: -5].split(' ] ,  [ ')
# >>> holdings_tokens[5]
# '"AMAZON.COM INC", "<button class=\\"modal_external appear-on-focus\\" href=\\"\\/modals\\/quick-quote.php\\" rel=\\"AMZN\\">AMZN Quick Quote<\\/button><a href=\\"\\/\\/www.zacks.com\\/funds\\/etf\\/AMZN\\" rel=\\"AMZN\\" class=\\" hoverquote-container-od \\" show-add-portfolio=\\"true\\" ><span class=\\"hoverquote-symbol\\">AMZN<span class=\\"sr-only\\"><\\/span><\\/span><\\/a>", "1,972", "4.36", "41.35", "<a class=\\"report_document newwin\\" href=\\"/zer/report/AMZN\\" alt=\\"View Report\\" title=\\"View Report\\"></a>"'
