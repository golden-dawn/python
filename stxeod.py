import csv
from datetime import datetime
from math import trunc
import os
import pandas as pd
from shutil import copyfile, rmtree
from stxcal import *
from stxdb import *
from stxts import StxTS
import sys

class StxEOD :

    sh_dir            = 'c:/goldendawn/stockhistory'
    upload_dir        = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    eod_name          = '{0:s}/eod_upload.txt'.format(upload_dir)
    sql_create_eod    = 'CREATE TABLE `{0:s}` ('\
                        '`stk` varchar(8) NOT NULL,'\
                        '`dt` varchar(10) NOT NULL,'\
                        '`o` decimal(9,2) DEFAULT NULL,'\
                        '`h` decimal(9,2) DEFAULT NULL,'\
                        '`l` decimal(9,2) DEFAULT NULL,'\
                        '`c` decimal(9,2) DEFAULT NULL,'\
                        '`v` int(11) DEFAULT NULL,'\
                        'PRIMARY KEY (`stk`,`dt`)'\
                        ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_split  = 'CREATE TABLE `{0:s}` ('\
                        '`stk` varchar(8) NOT NULL,'\
                        '`dt` varchar(10) NOT NULL,'\
                        '`ratio` decimal(8,4) DEFAULT NULL,'\
                        'PRIMARY KEY (`stk`,`dt`)'\
                        ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_split1 = 'CREATE TABLE `{0:s}` ('\
                        '`stk` varchar(8) NOT NULL,'\
                        '`dt` varchar(10) NOT NULL,'\
                        '`ratio` decimal(8,4) DEFAULT NULL,'\
                        '`implied` tinyint DEFAULT 0,'\
                        'PRIMARY KEY (`stk`,`dt`)'\
                        ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_recon  = 'CREATE TABLE `reconciliation` ('\
                        '`stk` varchar(8) NOT NULL,'\
                        '`recon_name` varchar(10) NOT NULL,'\
                        '`recon_interval` char(18) NOT NULL,'\
                        '`s_spot` char(10) DEFAULT NULL,'\
                        '`e_spot` char(10) DEFAULT NULL,'\
                        '`sdf` char(10) DEFAULT NULL,'\
                        '`edf` char(10) DEFAULT NULL,'\
                        '`splits` smallint DEFAULT NULL,'\
                        '`coverage` float DEFAULT NULL,'\
                        '`accuracy` float DEFAULT NULL,'\
                        '`status` tinyint DEFAULT 0,'\
                        'PRIMARY KEY (`stk`,`recon_name`,`recon_interval`)'\
                        ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    status_none       = 0
    status_ok         = 1
    status_ko         = 2

    def __init__(self, in_dir, eod_tbl, split_tbl, extension = '.txt') :
        self.in_dir    = in_dir
        self.eod_tbl   = eod_tbl
        self.split_tbl = split_tbl
        self.extension = extension
        db_create_missing_table(eod_tbl, self.sql_create_eod)
        print('EOD DB table: {0:s}'.format(eod_tbl))
        db_create_missing_table(split_tbl, self.sql_create_split1)
        print('Split DB table: {0:s}'.format(split_tbl))
        db_create_missing_table('reconciliation', self.sql_create_recon)


    # Load my historical data.  Load each stock and accumulate splits.
    # Upload splits at the end.
    def load_my_files(self, stx = '') :
        split_fname  = '{0:s}/my_splits.txt'.format(self.upload_dir)
        try :
            os.remove(split_fname)
            print('Removed {0:s}'.format(split_fname))
        except :
            pass
        if stx == '' :
            lst      = [f for f in os.listdir(self.in_dir) \
                        if f.endswith(self.extension)]
        else :
            lst      = []
            stk_list = stx.split(',')
            for stk in stk_list :
                lst.append('{0:s}{1:s}'.format(stk.strip(), self.extension))
        num_stx      = len(lst)
        print('Loading data for {0:d} stocks'.format(num_stx))
        ixx          = 0
        for fname in lst :
            self.load_my_stk(fname, split_fname)
            ixx     += 1
            if ixx % 500 == 0 or ixx == num_stx :
                print('Uploaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))
        try :
            db_upload_file(split_fname, self.split_tbl, 2)
            print('Successfully uploaded the splits in the DB')
        except :
            e = sys.exc_info()[1]
            print('Failed to upload the splits from file {0:s}, error {1:s}'.\
                  format(split_fname, str(e)))
            

    # Upload each stock.  Split lines are prefixed with '*'.  Upload
    # stock data separately and then accumulate each stock.
    def load_my_stk(self, short_fname, split_fname) :
        fname                   = '{0:s}/{1:s}'.format(self.in_dir, short_fname)
        stk                     = short_fname[:-4].upper()
        try :
            with open(fname, 'r') as ifile :
                lines           = ifile.readlines()
        except :
            e                   = sys.exc_info()[1]
            print('Failed to read {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        eods, splits            = 0, 0
        try :
            with open(self.eod_name, 'w') as eod :
                for line in lines:
                    start       = 1 if line.startswith('*') else 0
                    toks        = line[start:].strip().split(' ')
                    o,h,l,c,v   = float(toks[1]), float(toks[2]), \
                                  float(toks[3]), float(toks[4]), int(toks[5])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0 :
                        eod.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'\
                                  '{5:.2f}\t{6:d}\n'.\
                                  format(stk, toks[0], o, h, l, c, v))
                    eods       += 1
                    if start == 1 :
                        splits += 1
                        with open(split_fname, 'a') as split_file :
                            split_file.write('{0:s}\t{1:s}\t{2:f}\n'.format \
                                             (stk, toks[0], float(toks[6])))
        except :
            e                   = sys.exc_info()[1]
            print('Failed to parse {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        try :
            db_upload_file(self.eod_name, self.eod_tbl, 2)
            # print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
            #       format(stk, eods, splits))
        except :
            e                   = sys.exc_info()[1]
            print('Failed to upload {0:s}: {1:s}'.format(short_fname, str(e)))

        
    # Parse delta neutral stock history.  First, separate each yearly
    # data into stock files, and upload each stock file.  Then upload
    # all the splits into the database.  We will worry about
    # missing/wrong splits and volume adjustments later.
    def load_deltaneutral_files(self, stks = '') :
        if not os.path.exists(self.in_dir) :
            os.makedirs(self.in_dir)
        for yr in range(2001, 2017) :
            fname    = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx      = {}
            stk_list = [] if stks == '' else stks.split(',')
            with open(fname) as csvfile :
                frdr = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    if (stk_list and stk not in stk_list) or ('/' in stk) or \
                       ('*' in stk) or (stk in ['AUX', 'PRN']) :
                        continue
                    data = stx.get(stk, [])
                    o,h,l,c,v = float(row[2]), float(row[3]), float(row[4]), \
                                float(row[5]), int(row[7])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0 :
                        data.append('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'\
                                    '{5:.2f}\t{6:d}\n'.\
                                    format(stk, str(datetime.strptime\
                                                    (row[1],'%m/%d/%Y').date()),
                                           o, h, l, c, v))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items() :
                with open('{0:s}/{1:s}.txt'.format(self.in_dir, stk), 'a') \
                     as ofile :
                    for rec in recs :
                        ofile.write(rec)
        lst      = [f for f in os.listdir(self.in_dir) \
                    if f.endswith(self.extension)]
        for fname in lst :
            copyfile('{0:s}/{1:s}'.format(self.in_dir, fname), self.eod_name)
            try :
                db_upload_file(self.eod_name, self.eod_tbl, 2)
                print('{0:s}: uploaded eods'.format(stk))
            except :
                e = sys.exc_info()[1]
                print('Failed to upload {0:s}, error {1:s}'.format(stk, str(e)))
        self.load_deltaneutral_splits(stk_list)


    # Load the delta neutral splits into the database
    def load_deltaneutral_splits(self, stk_list) :
        # this part uploads the splits
        in_fname         = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        out_fname        = '{0:s}/stocksplits.txt'.format(self.upload_dir)
        with open(in_fname, 'r') as csvfile :
            with open(out_fname, 'w') as dbfile :
                frdr         = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    if stk_list and stk not in stk_list :
                        continue
                    dt   = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                    dbfile.write('{0:s}\t{1:s}\t{2:f}\n'.\
                                 format(stk,prev_busday(dt),1/float(row[2])))
        try :
            db_upload_file(out_fname, self.split_tbl, 2)
            print('Uploaded delta neutral splits')
        except :
            e = sys.exc_info()[1]
            print('Failed to upload splits, error {0:s}'.format(str(e)))


    # Perform reconciliation with the option spots.  First get all the
    # underliers for which we have spot prices within a given
    # interval.  Then, reconcile for each underlier
    def reconcile_spots(self, rec_name, sd = None, ed = None, stx = '',
                        dbg = False) :
        if stx == '' :
            res      = db_read_cmd('select distinct stk from opt_spots {0:s}'.\
                                   format(db_sql_timeframe(sd, ed, False)))
            stk_list = [stk[0] for stk in res]
        else :
            stk_list = stx.split(',')
        if sd is None :
            sd       = '2002-02-08'
        if ed is None :
            ed       = datetime.now.strftime('%Y-%m%d')
        rec_interval = '{0:s}_{1:s}'.format\
                       (datetime.strptime('sd', '%Y-%m-%d').strftime('%Y%m%d'),
                        datetime.strptime('ed', '%Y-%m-%d').strftime('%Y%m%d'))
        for stk in stk_list :
            res = self.reconcile_opt_spots(stk, sd, ed, dbg)
            if not dbg :
                db_write_cmd("insert into reconciliation values ('{0:s}',"\
                             "'{1:s}','{2:s}',{3:s},0".\
                             format(stk, rec_name, rec_interval, res))

    # Perform reconciliation for a single stock. If we cannot get the
    # EOD data, return N/A. Otherwise, return, for each stock, the
    # name, the start and end date between which spot data is
    # available, the start and end dates between which there is eod
    # data, and then the mse and percentage of coverage
    def reconcile_opt_spots(self, stk, sd, ed, dbg = False) :
        db_write_cmd("delete from {0:s} where stk = '{1:s}' and dt between "\
                     "'{2:s}' and '{3:s}' and implied = 1".\
                     format(self.split_tbl, stk, sd, ed))
        q                = "select dt, spot from opt_spots where stk='{0:s}' "\
                           "{1:s}".format(stk, db_sql_timeframe(sd, ed, True))
        spot_df          = pd.read_sql(q, db_get_cnx())
        spot_df.set_index('dt', inplace=True)
        s_spot, e_spot   = str(spot_df.index[0]), str(spot_df.index[-1])
        df, ts           = self.build_df_ts(spot_df, stk, sd, ed)
        if df is None :
            return '{0:s},{1:s},{2:s}{3:s}\n'.format(stk,s_spot,e_spot,',N/A'*5)
        df['r']          = df['spot'] / df['c']
        for i in [x for x in range(-3, 4) if x != 0] :
            df['r{0:d}'.format(i)]  = df['r'].shift(-i)
        df['rr']         = df['r1'] / df['r']
        df_f1            = df[(abs(df['rr'] - 1) > 0.05) & (df['c'] > 1.0) & \
                              (round(df['r-1'] - df['r'], 2) == 0) & \
                              (round(df['r-2'] - df['r'], 2) == 0) & \
                              (round(df['r-3'] - df['r'], 2) == 0) & \
                              (round(df['r2'] - df['r1'], 2) == 0) & \
                              (round(df['r3'] - df['r1'], 2) == 0)]
        for r in df_f1.iterrows() :
            db_write_cmd("insert into {0:s} values ('{1:s}', '{2:s}', "\
                         "{3:.4f}, 1)".format(self.split_tbl, stk, \
                                              str(r[0].date()), r[1]['rr']))
        df, cov, mse     = self.quality(df, ts, spot_df, dbg)
        if mse > 0.02 :
            df, ts       = self.autocorrect(df, spot_df, ts.stk, sd, ed)
            if df is None :
                return '{0:s},{1:s},{2:s}{3:s}\n'.format(stk, s_spot, e_spot,
                                                         ',N/A' * 5)
            df, cov, mse = self.quality(df, ts, spot_df, dbg)
        s_df, e_df       = str(df.index[0].date()), str(df.index[-1].date())
        if dbg :
            print('{0:s}: {1:s} {2:s} {3:s} {4:s} {5:s} {6:d} {7:.2f} {8:.4f}'.\
                  format(self.eod_tbl, stk, s_spot, e_spot, s_df, e_df,
                         len(df_f1), cov, mse))
            return df
        return "'{0:s}','{1:s}','{2:s}','{3:s}',{4:d},{5:.2f},{6:.4f}".\
            format(s_spot, e_spot, s_df, e_df, len(df_f1), cov, mse)    
    
    # Function that calculates the coverage and MSE between the spot
    # and eod prices
    def quality(self, df, ts, spot_df, dbg = False) :
        sfx           = '' if 'rr' in df.columns else '_adj'
        s_spot,e_spot = str(spot_df.index[0]), str(spot_df.index[-1])
        spot_days     = num_busdays(s_spot, e_spot)
        s_ts, e_ts    = str(ts.df.index[0].date()), str(ts.df.index[-1].date())
        if s_ts < s_spot :
            s_ts      = s_spot
        if e_ts > e_spot :
            e_ts      = e_spot
        ts_days       = num_busdays(s_ts, e_ts) if e_ts > s_ts else 0
        coverage      = round(100.0 * ts_days / spot_days, 2)
        # apply the split adjustments
        ts.splits.clear()
        splits        = db_read_cmd("select dt, ratio from {0:s} where stk = "\
                                    "'{1:s}' and implied = 1".\
                                    format(self.split_tbl, ts.stk))
        for s in splits:
            ts.splits[pd.to_datetime(next_busday(s[0]))] = float(s[1])
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv = 1)
        df.drop(['c'], inplace = True, axis = 1)
        df         = df.join(ts.df[['c']])
        # calculate statistics: coverage and mean square error
        msefun     = lambda x: 0 if x['spot'] == trunc(x['c']) or x['v'] == 0 \
                     or (x['spot'] == x['s1'] and x['spot'] == x['s2']) or \
                     (x['spot'] == x['s-1'] and x['spot'] == x['s-2']) else \
                     pow(1 - x['spot']/x['c'], 2)
        df['mse']  = df.apply(msefun, axis=1)
        accuracy   = pow(df['mse'].sum() / min(len(df['mse']), len(spot_df)),
                         0.5)
        if dbg :
            df.to_csv('c:/goldendawn/dbg/{0:s}_{1:s}{2:s}_recon.csv'.\
                      format(ts.stk, self.eod_tbl, sfx))
        return df, coverage, accuracy

    def cleanup(self) :
        db_write_cmd('drop table `{0:s}`'.format(self.eod_tbl))
        db_write_cmd('drop table `{0:s}`'.format(self.split_tbl))
        
    def cleanup_data_folder(self) :
        if os.path.exists(self.in_dir) :
            rmtree('{0:s}'.format(self.in_dir))

    def autocorrect(self, df, spot_df, stk, sd, ed) :
        df_err               = df.query('mse>0.01')
        start                = 0
        wrong_recs           = []
        split_adjs           = []
        while start < len(df_err) :
            mse0             = df_err.ix[start].mse
            strikes          = 0
            end, ixx         = start, start + 1
            while ixx < len(df_err) and strikes < 3 :
                rec          = df_err.ix[ixx]
                if rec.spot == trunc(rec.spot) :
                    pass
                elif abs(rec.mse - mse0) < 0.001 :
                    strikes  = 0
                    end      = ixx
                else :
                    strikes += 1
                ixx         += 1
            if end - start < 2 :
                wrong_recs.append(df_err.index[start])
                start       += 1
            else :
                rec          = df_err.ix[end]
                ratio        = rec.c / rec.spot
                start        = end + 1
                split_adjs.append(tuple([df_err.index[end], 1 / ratio]))
        print('wrong_recs = {0:s}'.format(str(wrong_recs)))
        print('split_adjs = {0:s}'.format(str(split_adjs)))
        return self.build_df_ts(spot_df, stk, sd, ed)

    
    def build_df_ts(self, spot_df, stk, sd, ed) :
        try :
            ts           = StxTS(stk, sd, ed, self.eod_tbl, self.split_tbl)
        except :
            return None, None
        df               = ts.df.join(spot_df)
        for i in [x for x in range(-2, 3) if x != 0] :
            df['s{0:d}'.format(i)]  = df['spot'].shift(-i)
        return df, ts

            
if __name__ == '__main__' :
    s_date = '2002-02-01'
    e_date = '2012-12-31'
    my_eod = StxEOD('c:/goldendawn/bkp', 'my_eod', 'my_split')
    # my_eod.load_my_files()
    dn_eod = StxEOD('c:/goldendawn/dn_data', 'dn_eod', 'dn_split')
    # dn_eod.load_deltaneutral_files()
    stx = 'AAI,AAMRQ,ABH,ABKFQ,ACL,ACLI,ACRT,ACW,AFN,AG,AH,AIB,AL,ALN,ALO,ALOY,AMCC,ANTP,ANX,AONE,APB,APWR,ARC,ARM,ART,ASF,ASYTQ,ATLS,ATN,ATRS,ATTC,AWC,AWE,AWK,BBC,BBIB,BDH,BER,BHH,BHS,BLC,BOX,BPUR,BRP,BSC,BW,BWC,BYT,CCME,CD,CDL,CDS,CEU,CGI,CHRW,CHTR,CHU,CIT,CML,CMVT,CNC,CNET,CPN,CPNLQ,CRGN,CRX,CRXX,CSR,CTC,CTDB,CTE,CTIC,DALRQ,DCGN,DCNAQ,DDX,DFX,DGW,DHBT,DJR,DLM,DOT,DPHIQ,DRL,DUX,DVS,DWSN,EEM,ENRNQ,ENT,EPIC,EPIX,ERES,ESCL,ESPR,EUR,EXB,EXE,EXXI,FBR,FCL,FDC,FEED,FNF,FOL,FRC,FRG,FRNTQ,FRP,FSE,FTO,FTS,FTWR,FVX,GAPTQ,GGC,GGP,GHA,GIN,GIP,GLK,GMEB,GOK,GOX,GPT,GRP,GSL,GSM,GSO,GSV,GTC,GTE,GTOP,GUC,HCH,HELX,HGX,HI,HK,HLS,HOFF,HQS,HRC,HS,HSOA,HSP,HWD,IDARQ,IDMCQ,IDT,IFS,IJJ,IMGC,IMH,IMPH,INET,INTL,INX,ION,IRX,IXX,IXZ,JAS,JAZZ,JDSU,JMBA,JPN,JSDA,KBK,KG,KRX,KSX,KVA,L,LEHMQ,LFB,LFGRQ,LMRA,LSX,LWIN,MERQ,MESA,MEX,MFX,MGG,MGM,MHR,MIR,MIRKQ,MKTY,MM,MND,MNG,MNST,MNX,MNY,MOX,MUT,MVR,NAL,NCOC,NEU,NEWCQ,NFLD,NOBL,NRTLQ,NSI,NSTR,NTLI,NTMD,NWACQ,NZ,NZT,OCLR,OCR,OEX,OIX,OMX,ORN,OSX,OVNT,P,PACT,PALM,PAR,PATH,PCMC,PCS,PCX,PDC,PDS,PIC,PKB,PLA,PMP,PMTC,POW,PPC,PRGN,PRM,PSTI,PVF,QI,QMNDQ,QRE,QTRN,RAG,RATL,RAV,RDG,RDSA,RDSB,RHT,RINO,RIO,RLG,RMC,RMG,RMN,RMV,RRR,RUA,RUI,RUJ,RUO,RXS,S,SAN,SAY,SCMR,SCON,SCOR,SCP,SCQ,SDL,SFC,SFUN,SHOP,SIN,SKIL,SMBL,SML,SMQ,SMRA,SNS,SOX,SPC,SPOT,SPSN,SSCC,SSI,STAR,STEL,STN,STQ,STSA,STTS,SUNH,SVM,T,TALX,TAM,TBI,TCM,TCP,TELK,TGH,TLGD,TMTA,TNX,TOMO,TOPS,TOUSQ,TPC,TRA,TREE,TRI,TRX,TSA,TXX,TYX,UAG,UAL,UTA,UTH,UTY,VC,VERT,VIAB,VION,VISG,VIV,VNBC,VNT,VOLV,VRA,WAC,WAMUQ,WCG,WCIMQ,WEN,WIN,WLDA,WLP,WM,WRC,XAU,XCI,XEO,XEX,XJT,XOI,XUE,YMI,YRCW,Z'
    my_eod.reconcile_spots(s_date, e_date, stx)
    dn_eod.reconcile_spots(s_date, e_date, stx)
    # To debug a reconciliation: 
    # my_eod.reconcile_opt_spots('AEOS', '2002-02-01', '2012-12-31', True)
