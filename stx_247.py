import argparse
import datetime
from github import Github
import glob
import json
import logging
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
import re
import smtplib
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS
from stx_plot import StxPlot
import sys
import time
import traceback
from weasyprint import HTML
import zipfile

class StxAnalyzer:
    def __init__(self):
        self.report_dir = os.path.join(os.getenv('HOME'), 'market')
        logging.info('PDF reports are stored locally in {0:s}'.
                     format(self.report_dir))
        self.git_report_dir = os.path.join(os.getenv('HOME'), 'reports')
        logging.info('PDF reports are stored in git from {0:s}'.
                     format(self.git_report_dir))
        self.report_style = '''
<style>
body {
  font-family: sans-serif;
  background-color: black;
  color: white;
}
table {
  border-collapse: collapse;
  border: 1px solid black;
  width: 100%;
  word-wrap: normal;
  table-layout: auto;
}
img {
  display: block;
  margin-left: auto;
  margin-right: auto;
  width: 99%;
}
</style>
'''

    def get_triggered_setups(self, dt):
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), sql.Literal(dt), 
                sql.SQL(' and setup in ('), sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(') and triggered='), sql.Literal(True)])
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def get_setups_for_tomorrow(self, dt):
        next_dt = stxcal.next_busday(dt)
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), 
                sql.Literal(next_dt), sql.SQL(' and setup in ('), 
                sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(')')])
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def get_high_activity(self, dt, df):
        eight_days_ago = stxcal.move_busdays(dt, -8)
        df['d_8'] = eight_days_ago
        def hiactfun(r):
            qha = sql.Composed(
                [sql.SQL('select * from setups where dt between '),
                 sql.Literal(r['d_8']), 
                 sql.SQL(' and '),
                 sql.Literal(r['dt']),
                 sql.SQL(' and stk='),
                 sql.Literal(r['stk']),
                 sql.SQL(' and setup in ('),
                 sql.SQL(',').join([sql.Literal('GAP_HV'), 
                                    sql.Literal('STRONG_CLOSE')]),
                 sql.SQL(')')])
            cnx = stxdb.db_get_cnx()
            rows = []
            with cnx.cursor() as crs:
                crs.execute(qha.as_string(cnx))
                rows = crs.fetchall()
            return len(rows) if rows else 0
        df['hi_act'] = df.apply(hiactfun, axis=1)

    def get_opt_spreads(self, crt_date, eod):
        exp_date = stxcal.next_expiry(crt_date, min_days=(1 if eod else 0))
        q = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
                                  'where expiry='), sql.Literal(exp_date)])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            crs.execute(q.as_string(cnx))
            spread_dict = {x[0]: x[1] for x in crs}
        return spread_dict

    def filter_spreads_hiact(self, df, spreads, max_spread):
        df['spread'] = df.apply(lambda r: spreads.get(r['stk']), axis=1)
        df.drop_duplicates(['stk', 'direction'], inplace=True)
        df = df[df.spread < max_spread]
        df = df[df.hi_act >= 3]
        df.sort_values(by=['direction', 'hi_act'], ascending=False, 
                       inplace=True)
        return df

    def get_report(self, crt_date, df):
        s_date = stxcal.move_busdays(crt_date, -50)
        res = []
        for _, row in df.iterrows():
            stk = row['stk']
            stk_plot = StxPlot(stk, s_date, crt_date)
            stk_plot.plot_to_file()
            res.append('<h4>{0:s}</h4>'.format(stk))
            res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                       format(stk, stk))
            ts = StxTS(stk, s_date, crt_date)
            day_ix = ts.set_day(crt_date)
            if day_ix == -1:
                continue
            avg_volume = np.average(ts.df['v'].values[-20:])
            rgs = [max(h, c_1) - min(l, c_1) 
                   for h, l, c_1 in zip(ts.df['hi'].values[-20:], 
                                        ts.df['lo'].values[-20:], 
                                        ts.df['c'].values[-21:-1])]
            avg_rg = np.average(rgs)
            res.append('<table border="1">')
            res.append('<tr><th>name</th><th>direction</th><th>spread'
                       '</th><th>avg_volume</th><th>avg_rg</th><th>hi_act'
                       '</th></tr>')
            res.append('<tr><td>{0:s}</td><td>{1:s}</td><td>{2:d}</td><td>'
                       '{3:,d}</td><td>{4:.2f}</td><td>{5:d}</td></tr>'.
                       format(stk, row['direction'], int(row['spread']),
                              int(1000 * avg_volume), avg_rg / 100, 
                              row['hi_act']))
            res.append('</table>')
        return res

    def do_analysis(self, crt_date, max_spread, eod):
        spreads = self.get_opt_spreads(crt_date, eod)
        df_1 = self.get_triggered_setups(crt_date)
        self.get_high_activity(crt_date, df_1)
        df_1 = self.filter_spreads_hiact(df_1, spreads, max_spread)
        res = ['<html>', self.report_style, '<body>']
        res.append('<h3>TODAY - {0:s}</h3>'.format(crt_date))
        res.extend(self.get_report(crt_date, df_1))
        if eod:
            df_2 = self.get_setups_for_tomorrow(crt_date)
            next_date = stxcal.next_busday(crt_date)
            self.get_high_activity(crt_date, df_2)
            df_2 = self.filter_spreads_hiact(df_2, spreads, max_spread)
            res.append('<h3>TOMMORROW - {0:s}</h3>'.format(next_date))
            res.extend(self.get_report(crt_date, df_2))
        res.append('</body>')
        res.append('</html>')
        with open('/tmp/x.html', 'w') as html_file:
            html_file.write('\n'.join(res))
        logging.info('Generated HTML report')
        time_now = datetime.datetime.now()
        time_now_date = '{0:d}-{1:02d}-{2:02d}'.format(time_now.year, 
                                                       time_now.month, 
                                                       time_now.day)
        suffix = 'EOD'
        if time_now_date == crt_date:
            if time_now.hour >= 10 and time_now.hour < 16:
                suffix = '{0:02d}{1:02d}'.format(time_now.hour,
                                                 time_now.minute)
            else:
                suffix = 'EOD'
        else:
            suffix = 'EOD' if eod else 'ID'
        pdf_fname = '{0:s}_{1:s}.pdf'.format(crt_date, suffix)
        logging.info('PDF report file name: {0:s}'.format(pdf_fname))
        pdf_filename = os.path.join(self.report_dir, pdf_fname)
        HTML(filename='/tmp/x.html').write_pdf(pdf_filename)
        logging.info('Saved report locally in {0:s}'.format(pdf_filename))
        git_pdf_filename = os.path.join(self.git_report_dir, pdf_fname)
        HTML(filename='/tmp/x.html').write_pdf(git_pdf_filename)
        logging.info('Saved report in git upload folder {0:s}'.
                     format(git_pdf_filename))
        return pdf_filename, git_pdf_filename

    def update_local_directory(self, crt_date, pdf_report):
        today_date = stxcal.today_date()
        start_of_current_month = '{0:s}{1:s}'.format(today_date[:8], '01')
        prev_month_date = stxcal.prev_busday(start_of_current_month)
        start_of_previous_month = '{0:s}{1:s}'.format(prev_month_date[:8], '01')
        zipfile_name = os.path.join(self.report_dir, '{0:s}.zip'.
            format(stxcal.prev_busday(start_of_previous_month)))
        logging.info('Will archive all the reports prior to {0:s} in {1:s}'.
            format(start_of_previous_month, zipfile_name))
        pdf_file_list = glob.glob(os.path.join(self.report_dir, '*.pdf'))
        zipfile_open_mode = 'a' if os.path.isfile(zipfile_name) else 'w'
        num_archived_pdfs = 0
        z = zipfile.ZipFile(zipfile_name, zipfile_open_mode)
        for pdf_file in pdf_file_list:
            short_filename = pdf_file.split(os.path.sep)[-1]
            if short_filename < start_of_previous_month:
                z.write(pdf_file)
                num_archived_pdfs += 1
                os.remove(pdf_file)
        z.close()
        logging.info('Archived {0:d} PDF reports in {1:s}'.
            format(num_archived_pdfs, zipfile_name))

    def update_git_directory(self, ana_date, git_pdf_report):
        today_date = stxcal.today_date()
        start_of_current_month = '{0:s}{1:s}'.format(today_date[:8], '01')
        prev_month_date = stxcal.prev_busday(start_of_current_month)
        start_of_previous_month = '{0:s}{1:s}'.format(prev_month_date[:8], '01')
        if ana_date < start_of_previous_month:
            logging.warn('Will not store in github reports before {0:s}'.
                format(start_of_previous_month))
            return
        g = Github(os.getenv('GIT_TOKEN'))
        git_user = g.get_user()
        repo = g.get_repo('{0:s}/reports'.format(git_user.login))
        git_files = [x.name for x in repo.get_contents('')
            if x.name != 'README.md'].sort()
        logging.info('Found {0:d} reports in the git repository'.
            format(len(git_files) if git_files else 0))
        first_date, last_date = None, None
        if git_files:
            first_date = git_files[0][:git_files[0].find('_')]
            last_date = git_files[-1][:git_files[-1].find('_')]
            logging.info('Git repo has reports from {0:s} to {1:s}'.
                format(first_date, last_date))
            logging.info('Checking for files in github older than {0:s}'.
                format(start_of_previous_month))
        logging.info('For now, no report uploading in git')
    # I should be able to encode and upload files to git using this:
    # filename = './2021-01-22_EOD.pdf'
    # with open(filename, 'rb') as pdf_file:
    #    pdf_b64_content = base64.b64encode(pdf_file.read())
    # pdf_str_content = pdf_b64_content.decode('utf-8')
    # blob = repo.create_git_blob(pdf_str_content, 'base64')
    # repo.create_file('test.txt', 'test', 'this is a test', branch='master')
    # Last thing doesn't work. It uploads PDF to github, but it cannot be visualized
    # repo.create_file('2021-01-22_EOD.pdf', 'test pdf report', pdf_str_content, branch='master')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--max_spread', type=int, default=33,
                        help='Maximum spread for leaders')
    parser.add_argument('-d', '--date', type=str, 
                        default=stxcal.current_busdate(hr=9),
                        help='Date to retrieve setups')
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    
    parser.add_argument('-c', '--cron', action='store_true',
                        help="Flag invocation from cron job")
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    analysis_type = 'Analysis'
    eod = False
    if args.cron:
        today_date = stxcal.today_date()
        if not stxcal.is_busday(today_date):
            logging.warn("stx_247 dont run on holidays ({0:s})".
                         format(today_date))
            sys.exit(0)
    if args.eod:
        analysis_type = 'EOD'
        eod = True
    if args.intraday:
        analysis_type = 'Intraday'
    if args.date:
        crt_date = args.date
    logging.info('Running analysis for {0:s}'.format(crt_date))
    stx_ana = StxAnalyzer()
    pdf_report, git_pdf_report = stx_ana.do_analysis(
        crt_date, args.max_spread, eod)
    stx_ana.update_local_directory(crt_date, pdf_report)
    stx_ana.update_git_directory(crt_date, git_pdf_report)