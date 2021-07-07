import argparse
import datetime
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
from stx_storage import GoogleDriveClient
import sys
import time
import traceback as tb
from weasyprint import HTML
import zipfile

class StxAnalyzer:
    def __init__(self):
        self.report_dir = os.path.join(os.getenv('HOME'), 'market')
        logging.info('PDF reports are stored locally in {0:s}'.
                     format(self.report_dir))
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
    def get_rs_stx(self, dt):
        q = sql.Composed([
                sql.SQL("select stk, indicators->>'rs' as rs, "
                        "indicators->>'rs_rank' as rs_rank from indicators"),
                sql.SQL(' where dt='),
                sql.Literal(dt),
                sql.SQL(' and stk not in (select * from excludes)')])
        rsdf = pd.read_sql(q, stxdb.db_get_cnx()) 
        rsdf[["rs", "rs_rank"]] = rsdf[["rs", "rs_rank"]].apply(pd.to_numeric)
        rsdf.sort_values(by=['rs'], ascending=False, inplace=True)
        return rsdf

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
                [sql.SQL('select * from jl_setups where dt between '),
                 sql.Literal(r['d_8']), 
                 sql.SQL(' and '),
                 sql.Literal(r['dt']),
                 sql.SQL(' and stk='),
                 sql.Literal(r['stk']),
                 sql.SQL(' and abs(score) > 100 and setup in ('),
                 sql.SQL(',').join([sql.Literal('Gap'),
                                    sql.Literal('SC'),
                                    sql.Literal('RDay')]),
                 sql.SQL(')')])
            db_df = pd.read_sql(qha, stxdb.db_get_cnx())
            return db_df['score'].sum() if len(db_df) > 0 else 0
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
#         df = df[df.hi_act >= 3]
        df.sort_values(by=['direction', 'hi_act'], ascending=False, 
                       inplace=True)
        return df

    def rs_report(self, i, row, s_date, jl_s_date, ana_s_date, crt_date):
        res = []
        stk = row['stk']
        stk_plot = StxPlot(stk, s_date, crt_date)
        stk_plot.plot_to_file()
        res.append('<h4>{}. {}, RS={}</h4>'.
                   format(i + 1, stk, row['rs']))
        res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.format(stk, stk))
        try:
            jl_res = StxJL.jl_report(stk, jl_s_date, crt_date, 1.5)
            res.append(jl_res)
        except:
            logging.error('JL(1.5) calc failed for {0:s}'.format(stk))
            tb.print_exc()
        try:
            ana_res = self.ana_report(stk, ana_s_date, crt_date)
            res.append(ana_res)
        except:
            logging.error('Failed to analyze {0:s}'.format(stk))
            tb.print_exc()
        return res

    def setup_report(self, row, s_date, jl_s_date, ana_s_date, crt_date):
        res = []
        try:
            stk = row['stk']
            stk_plot = StxPlot(stk, s_date, crt_date)
            stk_plot.plot_to_file()
            res.append('<h4>{0:s}</h4>'.format(stk))
            res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                       format(stk, stk))
            ts = StxTS(stk, s_date, crt_date)
            day_ix = ts.set_day(crt_date)
            if day_ix == -1:
                return []
            avg_volume = np.average(ts.df['v'].values[-20:])
            rgs = [max(h, c_1) - min(l, c_1)
                   for h, l, c_1 in zip(ts.df['hi'].values[-20:],
                                        ts.df['lo'].values[-20:],
                                        ts.df['c'].values[-21:-1])]
            avg_rg = np.average(rgs)
            res.append('<table border="1">')
            res.append('<tr><th>name</th><th>dir</th><th>spread'
                       '</th><th>avg_volume</th><th>avg_rg</th><th>hi_act'
                       '</th><th>rs</th><th>rs_rank</th></tr>')
            res.append('<tr><td>{0:s}</td><td>{1:s}</td><td>{2:d}</td><td>'
                       '{3:,d}</td><td>{4:.2f}</td><td>{5:d}</td>'
                       '<td>{6:d}</td><td>{7:d}</td></tr>'.
                       format(stk, row['direction'], int(row['spread']),
                              int(1000 * avg_volume), avg_rg / 100,
                              row['hi_act'], row['rs'], row['rs_rank']))
            res.append('</table>')
        except:
            logging.error('Failed analysis for {0:s}'.format(stk))
            tb.print_exc()
            return []
        try:
            jl_res = StxJL.jl_report(stk, jl_s_date, crt_date, 1.5)
            res.append(jl_res)
        except:
            logging.error('{0:s} JL(1.5) calc failed'.format(stk))
            tb.print_exc()
        try:
            ana_res = self.ana_report(stk, ana_s_date, crt_date)
            res.append(ana_res)
        except:
            logging.error('Failed to analyze {0:s}'.format(stk))
            tb.print_exc()
        return res

    def get_report(self, crt_date, df, do_analyze):
        s_date = stxcal.move_busdays(crt_date, -50)
        jl_s_date = stxcal.move_busdays(crt_date, -350)
        ana_s_date = stxcal.move_busdays(crt_date, -20)
        res = []
        rsdf = self.get_rs_stx(crt_date)
        if do_analyze:
            indexes = ['^GSPC', '^IXIC', '^DJI']
            for index in indexes:
                stk_plot = StxPlot(index, s_date, crt_date)
                stk_plot.plot_to_file()
                res.append('<h4>{0:s}</h4>'.format(index))
                res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                           format(index, index))
                try:
                    jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 1.0)
                    res.append(jl_res)
                except:
                    logging.error('{0:s} JL(1.0) calc failed'.format(index))
                    tb.print_exc()
                try:
                    jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 2.0)
                    res.append(jl_res)
                except:
                    logging.error('{0:s} JL(2.0) calc failed'.format(index))
                    tb.print_exc()
                try:
                    ana_res = self.ana_report(index, ana_s_date, crt_date)
                    res.append(ana_res)
                except:
                    logging.error('Failed to analyze {0:s}'.format(index))
                    tb.print_exc()
        setup_df = df.merge(rsdf)
        up_setup_df = setup_df.query("direction=='U'").copy()
        up_setup_df.sort_values(by=['rs'], ascending=False, inplace=True)
        down_setup_df = setup_df.query("direction=='D'").copy()
        down_setup_df.sort_values(by=['rs'], ascending=True, inplace=True)
        res.append('<h3>{0:d} UP Setups</h3>'.format(len(up_setup_df)))
        for _, row in up_setup_df.iterrows():
            res.extend(self.setup_report(row, s_date, jl_s_date, ana_s_date,
                                         crt_date))
        res.append('<h3>{0:d} DOWN Setups</h3>'.format(len(down_setup_df)))
        for _, row in down_setup_df.iterrows():
            res.extend(self.setup_report(row, s_date, jl_s_date, ana_s_date,
                                         crt_date))
        if do_analyze:
            rsbest = rsdf.query('rs_rank==99').copy()
            rsworst = rsdf.query('rs_rank==0').copy()
            rsworst.sort_values(by=['rs'], ascending=True, inplace=True)
            res.append('<h3>RS Leaders</h3>')
            for i, (_, row) in enumerate(rsbest.iterrows()):
                res.extend(self.rs_report(i, row, s_date, jl_s_date,
                                          ana_s_date, crt_date))
            res.append('<h3>RS Laggards</h3>')
            for i, (_, row) in enumerate(rsworst.iterrows()):
                res.extend(self.rs_report(i, row, s_date, jl_s_date,
                                          ana_s_date, crt_date))

        return res

    def do_analysis(self, crt_date, max_spread, eod):
        spreads = self.get_opt_spreads(crt_date, eod)
        df_1 = self.get_triggered_setups(crt_date)
        self.get_high_activity(crt_date, df_1)
        df_1 = self.filter_spreads_hiact(df_1, spreads, max_spread)
        res = ['<html>', self.report_style, '<body>']
        res.append('<h3>TODAY - {0:s}</h3>'.format(crt_date))
        res.extend(self.get_report(crt_date, df_1, True))
        if eod:
            df_2 = self.get_setups_for_tomorrow(crt_date)
            next_date = stxcal.next_busday(crt_date)
            self.get_high_activity(crt_date, df_2)
            df_2 = self.filter_spreads_hiact(df_2, spreads, max_spread)
            res.append('<h3>TOMMORROW - {0:s}</h3>'.format(next_date))
            res.extend(self.get_report(crt_date, df_2, False))
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
        return pdf_filename

    def ana_report(self, stk, start_date, end_date):
        res = '<table><tr>'
        jl_start_date = stxcal.move_busdays(end_date, -8)
        # add the A/D setups table
        res += '<td><table>'
        qad = sql.Composed(
            [sql.SQL('select * from jl_setups where dt between '),
             sql.Literal(start_date),
             sql.SQL(' and '),
             sql.Literal(end_date),
             sql.SQL(' and setup in ('),
             sql.SQL(',').join([sql.Literal('Gap'),
                                sql.Literal('SC'),
                                sql.Literal('RDay')]),
             sql.SQL(') and abs(score) >= 100 and stk='),
             sql.Literal(stk),
             sql.SQL(' order by dt, direction, setup')])
        df_ad = pd.read_sql(qad, stxdb.db_get_cnx())
        for _, row in df_ad.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'\
                '</tr>'.format(row['dt'].strftime('%b %d'), row['setup'],
                               row['direction'], row['score'])
        res += '</td></table>'
        # add the JL setups table
        res += '<td><table>'
        qjl = sql.Composed(
            [sql.SQL('select * from jl_setups where dt between '),
             sql.Literal(jl_start_date),
             sql.SQL(' and '),
             sql.Literal(end_date),
             sql.SQL(' and setup in ('),
             sql.SQL(',').join([sql.Literal('JL_B'),
                                sql.Literal('JL_P'),
                                sql.Literal('JL_SR')]),
             sql.SQL(') and stk='),
             sql.Literal(stk),
             sql.SQL(' order by dt, direction, setup, factor')])
        df_jl = pd.read_sql(qjl, stxdb.db_get_cnx())
        for _, row in df_jl.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'\
                '<td>{}</td></tr>'.format(row['dt'].strftime('%b %d'),
                                          row['setup'], row['direction'],
                                          row['factor'], row['score'])
        res += '</table></td>'
        # add the candlesticks setups table
        res += '<td><table>'
        qcs = sql.Composed(
            [sql.SQL('select * from jl_setups where dt between '),
             sql.Literal(start_date),
             sql.SQL(' and '),
             sql.Literal(end_date),
             sql.SQL(' and setup in ('),
             sql.SQL(',').join([sql.Literal('EngHarami'),
                                sql.Literal('Cbs'),
                                sql.Literal('3out'),
                                sql.Literal('3'),
                                sql.Literal('Kicking'),
                                sql.Literal('Piercing'),
                                sql.Literal('Engulfing'),
                                sql.Literal('Star')]),
             sql.SQL(') and stk='),
             sql.Literal(stk),
             sql.SQL(' order by dt, direction, setup')])
        df_cs = pd.read_sql(qcs, stxdb.db_get_cnx())
        for _, row in df_cs.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
                row['dt'].strftime('%b %d'), row['setup'], row['direction'])
        res += '</td></table>'
        res += '</tr></table>'
        return res

    def update_local_directory(self, crt_date, pdf_report):
        today_date = stxcal.today_date()
        start_of_current_month = '{0:s}{1:s}'.format(today_date[:8], '01')
        prev_month_date = stxcal.prev_busday(start_of_current_month)
        start_of_prev_month = '{0:s}{1:s}'.format(prev_month_date[:8], '01')
        zipfile_name = os.path.join(self.report_dir, '{0:s}.zip'.
            format(stxcal.prev_busday(start_of_prev_month)))
        logging.info('Will archive all the reports prior to {0:s} in {1:s}'.
            format(start_of_prev_month, zipfile_name))
        pdf_file_list = glob.glob(os.path.join(self.report_dir, '*.pdf'))
        zipfile_open_mode = 'a' if os.path.isfile(zipfile_name) else 'w'
        num_archived_pdfs = 0
        z = zipfile.ZipFile(zipfile_name, zipfile_open_mode)
        for pdf_file in pdf_file_list:
            short_filename = pdf_file.split(os.path.sep)[-1]
            if short_filename < start_of_prev_month:
                z.write(pdf_file)
                num_archived_pdfs += 1
                os.remove(pdf_file)
        z.close()
        logging.info('Archived {0:d} PDF reports in {1:s}'.
            format(num_archived_pdfs, zipfile_name))


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
    pdf_report = stx_ana.do_analysis(crt_date, args.max_spread, eod)
    gdc = GoogleDriveClient()
    gdc.upload_report(pdf_report, os.path.basename(pdf_report))
    stx_ana.update_local_directory(crt_date, pdf_report)
