import argparse
import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import json
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
import re
import smtplib
# import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS
import sys
import time
import traceback
from weasyprint import HTML

class StxMail:

    def __init__(self):
        pass

#         for stk in stx:
#             ts = StxTS(stk, self.start_date, ana_date)
#             for ixx in range(1, 5):
#                 ts.df['hi_{0:d}'.format(ixx)] = ts.df['hi'].shift(ixx)
#                 ts.df['lo_{0:d}'.format(ixx)] = ts.df['lo'].shift(ixx)
#             ts.df['v_50'] = ts.df['volume'].rolling(50).mean()

#     def mail_analysis(self, analysis_type):
# #         crt_date = stxcal.current_busdate(hr=9)
# #         exp_date = stxcal.next_expiry(crt_date)
#         smtp_server = os.getenv('EMAIL_SERVER')
#         smtp_user = os.getenv('EMAIL_USER')
#         smtp_passwd = os.getenv('EMAIL_PASSWD')
#         smtp_email = os.getenv('EMAIL_USER')
#         smtp_port = os.getenv('EMAIL_PORT')
#         res = '{0:6s} {1:9s} {2:6s} {3:12s} {4:6s}'.format(
#             'name', 'direction', 'spread', 'avg_volume', 'avg_rg')
#         #      dt     |  stk  |  setup   | direction | triggered 
#         # ------------+-------+----------+-----------+-----------
#         #  2019-08-21 | MSM   | JC_1234  | D         | t
#         q1 = sql.Composed([sql.SQL('select * from setups where dt='),
#                           sql.Literal(crt_date)])
#         df = pd.read_sql(q1, stxdb.db_get_cnx())
#         q2 = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
#                                    'where expiry='), sql.Literal(exp_date)])
#         cnx = stxdb.db_get_cnx()
#         with cnx.cursor() as crs:
#             crs.execute(q2.as_string(cnx))
#             spread_dict = {x[0]: x[1] for x in crs}
#         df['spread'] = df.apply(lambda r: spread_dict.get(r['stk']), axis=1)
#         df.drop_duplicates(['stk', 'direction'], inplace=True)
#         df.sort_values(by=['direction', 'spread'], inplace=True)
#         s_date = stxcal.move_busdays(crt_date, -49)
#         for _, row in df.iterrows():
#             stk = row['stk']
#             ts = StxTS(stk, s_date, crt_date)
#             avg_volume = np.average(ts.df['v'].values[:])
#             rgs = [max(h, c_1) - min(l, c_1) 
#                    for h, l, c_1 in zip(ts.df['hi'].values[-20:], 
#                                         ts.df['lo'].values[-20:], 
#                                         ts.df['c'].values[-21:-1])]
#             avg_rg = np.average(rgs)
#             res = '{0:s}\r\n{1:6s} {2:9s} {3:6d} {4:12,d} {5:6.2f}'.format(
#                     res, stk, row['direction'], row['spread'],
#                     int(1000 * avg_volume), avg_rg / 100)
#         try:
#             try:
#                 s = smtplib.SMTP(host=smtp_server, port=smtp_port)
#                 s.starttls()
#                 s.login(smtp_user, smtp_passwd)
#                 msg = MIMEText(res, 'plain')
#                 msg['Subject'] = '{0:s} {1:s}'.format(analysis_type, crt_date)
#                 msg['From'] = smtp_email
#                 msg['To'] = smtp_email
#                 s.sendmail(smtp_email, smtp_email, msg.as_string())
#             except:
#                 print('Something failed: {0:s}'.format(traceback.print_exc()))
#             finally:
#                 s.quit()
#         except:
#             print('Failed to send email: {0:s}'.format(traceback.print_exc()))

    def mail_test(self):
#         crt_date = stxcal.current_busdate(hr=9)
#         exp_date = stxcal.next_expiry(crt_date)
        smtp_server = os.getenv('EMAIL_SERVER')
        smtp_user = os.getenv('EMAIL_USER')
        smtp_passwd = os.getenv('EMAIL_PASSWD')
        smtp_email = os.getenv('EMAIL_USER')
        smtp_port = os.getenv('EMAIL_PORT')

        # Create the body of the message (a plain-text and an HTML version).
        html = """\
<html>
<style scoped>
.whiteText {background-color:black;color:white;}
</style>
  <head></head>
  <body>
    <p class="whiteText">Hi!<br>
       <span style="color:#FF0000"><u>How</u></span> are you?<br>
       Here is the <a href="http://www.python.org">link</a> you wanted.
    </p>
  </body>
</html>
"""
        report_style = '''
<style>
h3 {
  font-family: sans-serif;
}
h4 {
  font-family: sans-serif;
}
table {
  border-collapse: collapse;
  border: 1px solid black;
  width: 100%;
  word-wrap: normal;
  font-family: sans-serif;
}
table.a {
  table-layout: auto;
}
table.b {
  table-layout: fixed;
}
</style>
'''
        x = [report_style]
        x.append('<table class="b" border="1">')
        x.append('<tr><th>Date</th><th>NRa</th><th>UT</th><th>DT</th>'
                 '<th>NRe</th><th>OBV</th><th>RG</th></tr>')
        x.append("<tr><td>2019-09-13</td>"
                 "<td><span style='color:#FF0000'><u>65.00</u></span></td>"
                 "<td></td><td></td><td></td><th>2.3</td><td>1.23</td></tr>")
        x.append("<tr><td>2019-09-20</td><td></td><td></td>"
                 "<td><span style='color:#FF0000'><u>45.00</u></span></td>"
                 "<td></td><th>12.3</td><td>2.33</td></tr>")
        x.append('</table>')
        x.append('<img src="aapl.png" alt="img1">')
        x.append('<table class="b" border="1">')
        x.append('<tr><th>Date</th><th>NRa</th><th>UT</th><th>DT</th>'
                 '<th>NRe</th><th>OBV</th><th>RG</th></tr>')
        x.append("<tr><td>2019-09-13</td>"
                 "<td><span style='color:#FF0000'><u>65.00</u></span></td>"
                 "<td></td><td></td><td></td><th>2.3</td><td>1.23</td></tr>")
        x.append("<tr><td>2019-09-20</td><td></td><td></td>"
                 "<td><span style='color:#FF0000'><u>45.00</u></span></td>"
                 "<td></td><th>12.3</td><td>2.33</td></tr>")
        x.append('</table>')
        x.append('<img src="aapl1.png" alt="img2">')
        html = '\n'.join(x)
        with open('x.html', 'w') as f:
            f.write(html)
        HTML(filename='x.html').write_pdf('x.pdf')
        # Record the MIME types of both parts - text/plain and text/html.
#         part1 = MIMEText(text, 'plain')
#         part2 = MIMEText(html, 'html')

#         res = '{0:6s} {1:9s} {2:6s} {3:12s} {4:6s}'.format(
#             'name', 'direction', 'spread', 'avg_volume', 'avg_rg')
#         #      dt     |  stk  |  setup   | direction | triggered 
#         # ------------+-------+----------+-----------+-----------
#         #  2019-08-21 | MSM   | JC_1234  | D         | t
#         q1 = sql.Composed([sql.SQL('select * from setups where dt='),
#                           sql.Literal(crt_date)])
#         df = pd.read_sql(q1, stxdb.db_get_cnx())
#         q2 = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
#                                    'where expiry='), sql.Literal(exp_date)])
#         cnx = stxdb.db_get_cnx()
#         with cnx.cursor() as crs:
#             crs.execute(q2.as_string(cnx))
#             spread_dict = {x[0]: x[1] for x in crs}
#         df['spread'] = df.apply(lambda r: spread_dict.get(r['stk']), axis=1)
#         df.drop_duplicates(['stk', 'direction'], inplace=True)
#         df.sort_values(by=['direction', 'spread'], inplace=True)
#         s_date = stxcal.move_busdays(crt_date, -49)
#         for _, row in df.iterrows():
#             stk = row['stk']
#             ts = StxTS(stk, s_date, crt_date)
#             avg_volume = np.average(ts.df['v'].values[:])
#             rgs = [max(h, c_1) - min(l, c_1) 
#                    for h, l, c_1 in zip(ts.df['hi'].values[-20:], 
#                                         ts.df['lo'].values[-20:], 
#                                         ts.df['c'].values[-21:-1])]
#             avg_rg = np.average(rgs)
#             res = '{0:s}\r\n{1:6s} {2:9s} {3:6d} {4:12,d} {5:6.2f}'.format(
#                     res, stk, row['direction'], row['spread'],
#                     int(1000 * avg_volume), avg_rg / 100)
        try:
            try:
                s = smtplib.SMTP(host=smtp_server, port=smtp_port)
                s.starttls()
                s.login(smtp_user, smtp_passwd)
                msg = MIMEMultipart()
                with open('x.pdf', 'rb') as fpdf:
                    pdf = MIMEApplication(fpdf.read(), Name='x.pdf')
                pdf['Content-Disposition'] = 'attachment; filename="x.pdf"'
                msg.attach(pdf)

#                 with open('aapl1.png', 'rb') as fimg:
#                     msg_image = MIMEImage(fimg.read(), name='aapl1.png')
#                     msg_image.add_header('Content-ID', '0')
#                     msg_image.add_header("Content-Disposition", "in-line", 
#                                          filename='aapl1.png')
#                     msg_image.add_header('X-Attachment-Id', '0')  
#                     msg.attach(msg_image)
#                 msg_html = MIMEText(html, 'html')
#                 msg.attach(msg_html)
#                 with open('aapl.png', 'rb') as fimg:
#                     msg_image_1 = MIMEImage(fimg.read())
#                     msg_image_1.add_header('Content-ID', '1')
#                     msg_image_1.add_header("Content-Disposition", "in-line", 
#                                            filename='aapl.png')
#                     msg_image_1.add_header('X-Attachment-Id', '1')  
#                     msg.attach(msg_image_1)
#                 msg = MIMEText(html, 'html')
#                 msg.attach(part1)
#                 msg.attach(part2)
#                 msg['Subject'] = '{0:s} {1:s}'.format(analysis_type, crt_date)
                msg['Subject'] = 'This is a multicolor test'
                msg['From'] = smtp_email
                msg['To'] = smtp_email
                s.sendmail(smtp_email, smtp_email, msg.as_string())
            except:
                print('Something failed: {0:s}'.format(traceback.print_exc()))
            finally:
                s.quit()
        except:
            print('Failed to send email: {0:s}'.format(traceback.print_exc()))

        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--leaders', action='store_true',
                        help='Require leader calculation')
    parser.add_argument('-s', '--setups', action='store_true',
                        help='Require setup calculation')
    parser.add_argument('-o', '--get-options', action='store_true',
                        help='Retrieve option prices')
    parser.add_argument('-a', '--min_act', type=int,
                        help='Minimum activity for leaders')
    parser.add_argument('-r', '--range_close_ratio', type=float,
                        help='Minimum range to close ratio for leaders')
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    
    args = parser.parse_args()
    analysis_type = 'EOD'
    if args.eod:
        analysis_type = 'EOD'
    if args.intraday:
        analysis_type = 'Intraday'
    stx_mail = StxMail()
    stx_mail.mail_test()
#     stx_mail.mail_analysis(analysis_type)
