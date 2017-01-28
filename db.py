stxdb.db_write_cmd("delete from eod where stk='ABVT'")
stxdb.db_write_cmd("delete from eod where stk='AFN' and dt between '2009-10-23' and '2009-10-27'")
stxdb.db_write_cmd("insert into split values ('AGI', '2002-04-09', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('AI', '2002-11-01', 100.0000, 0)")
stxdb.db_write_cmd("insert into split values ('ALEX', '2012-06-29', 0.50, 0)")
stxdb.db_write_cmd("delete from eod where stk='ALO' and dt between '2008-05-06' and '2008-05-22'")
stxdb.db_write_cmd("update split set dt='2006-02-10' where stk='ALOY' and dt='2006-01-31'")
stxdb.db_write_cmd("update split set dt='2007-12-21' where stk='AMCC' and dt='2007-12-10'")
stxdb.db_write_cmd("update split set dt='2012-07-23' where stk='AMPL' and dt='2012-07-20'")
stxdb.db_write_cmd("update split set dt='2006-04-19' where stk='APPX' and dt='2006-04-18'")
stxdb.db_write_cmd("insert into split values ('APPYD', '2011-07-28', 5.0, 0)")
stxdb.db_write_cmd("insert into split values ('AT', '2006-07-17', 0.83, 0)")
stxdb.db_write_cmd("insert into split values ('AWI', '2010-12-10', 0.75, 0)")
stxdb.db_write_cmd("insert into split values ('AXAHY', '2001-05-16', 0.500, 0)")
stxdb.db_write_cmd("insert into split values ('BAH', '2012-08-31', 0.6667, 0)")
stxdb.db_write_cmd("insert into split values ('BANRD', '2011-05-31', 6.0, 0)")
stxdb.db_write_cmd("insert into split values ('BBH', '2009-03-31', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('BCO', '2008-10-31', 0.5, 0)")
stxdb.db_write_cmd("update split set dt='2003-11-21' where stk='BKE' and dt='2003-11-20'")
stxdb.db_write_cmd("insert into split values ('BTY', '2001-11-15', 0.75, 0)")
stxdb.db_write_cmd("insert into split values ('BZF', '2011-12-20', 0.75, 0)")
stxdb.db_write_cmd("insert into split values ('CAGC', '2009-09-04', 4.5, 0)")
stxdb.db_write_cmd("insert into split values ('CAH', '2009-08-31', 0.718,,2)")
stxdb.db_write_cmd("insert into split values ('CCE', '2010-10-01', 0.6855, 2)")
stxdb.db_write_cmd("insert into split values ('CCMP', '2012-03-01', 0.7111, 2)")
stxdb.db_write_cmd("insert into split values ('CDCAQ', '2010-08-20', 3.0, 0)")
stxdb.db_write_cmd("delete from eod where stk='CDZI'")
stxdb.db_write_cmd("insert into split values ('CEL', '2003-05-09', 10, 0)")
stxdb.db_write_cmd("delete from eod where stk='CGV' and dt<'2006-08-11'")
stxdb.db_write_cmd("delete from eod where stk='CHIB'")
stxdb.db_write_cmd("update split set dt='2005-10-21' where stk='CHRW' and dt='2005-10-17'")
stxdb.db_write_cmd("insert into split values ('CHT', '2011-01-28', 1.25, 0)")
stxdb.db_write_cmd("insert into split values ('CKEC', '2002-02-01', 4.0, 0)")
stxdb.db_write_cmd("delete from eod where stk='CN' and dt='2001-04-30'")
stxdb.db_write_cmd("delete from eod where stk='CN' and dt='2001-05-07'")
stxdb.db_write_cmd("insert into split values ('CNH', '2012-12-17', 0.8230, 0)")
stxdb.db_write_cmd("insert into split values ('CNXT', '2008-06-27', 10, 0)")
stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, v=2*v/3 where stk='COLM' and dt='2001-04-30'")
stxdb.db_write_cmd("insert into split values ('CTC', '2004-09-03', 0.8, 0)")
stxdb.db_write_cmd("insert into split values ('CTR', '2005-06-27', 0.6669, 0)")
stxdb.db_write_cmd("insert into split values ('CVC', '2006-04-24', 0.667, 0)")
stxdb.db_write_cmd("delete from eod where stk='CZM'")
stxdb.db_write_cmd("insert into split values ('DEXO', '2010-05-27', 0.6615, 0)")
stxdb.db_write_cmd("insert into split values ('DNN', '2006-12-06', 0.33, 0)")
stxdb.db_write_cmd("insert into split values ('DPTRD', '2011-07-12', 9.0, 0)")
stxdb.db_write_cmd("insert into split values ('DPTRQ', '2011-07-12', 9.0, 0)")
stxdb.db_write_cmd("insert into split values ('DXD', '2008-12-22', 0.7705, 0)")
stxdb.db_write_cmd("insert into split values ('DYNIQ', '2010-05-24', 4.5, 0)")
stxdb.db_write_cmd("insert into split values ('DZK', '2009-11-19', 0.8357, 0)")
stxdb.db_write_cmd("insert into split values ('ECA', '2009-12-08', 0.5351, 0)")
stxdb.db_write_cmd("delete from split where stk='ECYT'")
stxdb.db_write_cmd("insert into split values ('EDMC', '2012-02-01', 0.8438, 0)")
stxdb.db_write_cmd("delete from eod where stk='EPIC' and dt in ('2003-10-07', '2003-10-08')")
stxdb.db_write_cmd("delete from eod where stk='EPIC' and dt in ('2003-11-03', '2003-11-04')")
stxdb.db_write_cmd("delete from eod where stk='EPIC' and dt in ('2003-11-03', '2003-11-04')")
stxdb.db_write_cmd("delete from eod where stk='EPIC' and dt between '2004-01-14' and '2004-01-15'")
stxdb.db_write_cmd("delete from eod where stk='EPIC' and dt='2007-07-03'")
stxdb.db_write_cmd("insert into split values ('EQU', '2010-05-28', 3.0, 0)")
stxdb.db_write_cmd("delete from eod where stk='ERES' and dt>'2010-02-22'")
stxdb.db_write_cmd("delete from eod where stk='EXB'")
stxdb.db_write_cmd("insert into split values ('EXH', '2010-08-05', 0.8388, 0)")
stxdb.db_write_cmd("insert into split values ('FBR', '2009-03-26', 25.0, 0)")
stxdb.db_write_cmd("insert into split values ('FNP', '2002-01-16', 0.50, 0)")
stxdb.db_write_cmd("insert into split values ('FST', '2006-03-02', 0.6700, 0)")
stxdb.db_write_cmd("insert into split values ('FST', '2011-09-30', 0.7174, 0)")
stxdb.db_write_cmd("update split set dt='2009-12-24' where stk='FTWR' and dt='2009-12-21'")
stxdb.db_write_cmd("update eod set o=2*o, h=2*h, l=2*l, c=2*c, v=v/2 where stk='GENZ' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=2*o, h=2*h, l=2*l, c=2*c, v=v/2 where stk='GENZ' and dt='2001-05-07'")
stxdb.db_write_cmd("delete from eod where stk='GEOY'")
stxdb.db_write_cmd("insert into split values ('GFRE', '2009-10-09', 4.25, 0)")
stxdb.db_write_cmd("insert into split values ('GOK', '2006-11-09', 10.0, 0)")
stxdb.db_write_cmd("insert into split values ('HEES', '2012-09-19', 0.6468,2)")
stxdb.db_write_cmd("delete from eod where stk='HIL' and dt between '2007-12-12' and '2007-12-18'")
stxdb.db_write_cmd("update split set dt='2006-10-31' where stk='HLS' and dt='2006-10-26'")
stxdb.db_write_cmd("insert into split values ('HQSM', '2007-01-30', 20.0000, 0)")
stxdb.db_write_cmd("insert into split values ('HRZL', '2011-12-19', 15.0000, 0)")
stxdb.db_write_cmd("insert into split values ('HSON', '2005-02-25', 0.5, 0)")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='HTLD' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='HTLD' and  dt='2001-05-07'")
stxdb.db_write_cmd("insert into split values ('HTX', '2007-06-29', 0.6157, 2)")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='IBOC' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='IBOC' and dt='2001-05-07'")
stxdb.db_write_cmd("insert into split values ('IDT', '2011-10-28', 0.5825, 2)")
stxdb.db_write_cmd("insert into split values ('IDTC', '2001-05-31', 0.5530, 2)")
stxdb.db_write_cmd("insert into split values ('IRE', '2011-10-14', 8.5, 0)")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='IVX' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=1.25*o, h=1.25*h, l=1.25*l, c=1.25*c, v=0.8*v where stk='IVX' and dt='2001-05-07'")
stxdb.db_write_cmd("insert into split values ('JAVA', '2007-11-27', 4.0, 0)")
stxdb.db_write_cmd("update split set dt='2006-10-27' where stk='JDSU' and dt='2006-10-17'")
stxdb.db_write_cmd("delete from eod where stk='JRCC' and dt <'2004-11-17'")
stxdb.db_write_cmd("insert into split values ('KND', '2007-07-31', 0.8, 0)")
stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, v=2*v/3 where stk='KNGT' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, v=2*v/3 where stk='KNGT' and dt='2001-05-07'")
stxdb.db_write_cmd("delete from eod where stk='KRX' and dt<'2009-03-25'")
stxdb.db_write_cmd("insert into split values ('KV.A', '2003-09-29', 0.6741, 0)")
stxdb.db_write_cmd("insert into split values ('L', '2004-06-07', 0.8399, 2)")
stxdb.db_write_cmd("update split set dt='2005-12-27' where stk='LJPC' and dt='2005-12-22'")
stxdb.db_write_cmd("insert into eod values ('LMNX', '2002-02-01', 14.20,14.40,12.90,13.04,1748700)")
stxdb.db_write_cmd("insert into split values ('LORL', '2012-12-04', 0.667, 0)")
stxdb.db_write_cmd("insert into split values ('MIICF', '2004-02-20', 0.25, 0)")
stxdb.db_write_cmd("insert into split values ('MOGA', '2001-09-21', 0.6697, 0)")
stxdb.db_write_cmd("delete from eod where stk='MOGA' and dt>'2003-11-05'")
stxdb.db_write_cmd("insert into split values ('MOVED', '2011-11-18', 3.6623, 0)")
stxdb.db_write_cmd("insert into split values ('MWN', '2009-06-24', 2.0, 0)")
stxdb.db_write_cmd("delete from eod where stk='NAL'")
stxdb.db_write_cmd("update split set dt='2005-01-13' where stk='NCOC' and dt='2005-01-12'")
stxdb.db_write_cmd("delete from eod where stk='NETC' and dt between '2006-07-31' and '2006-09-06'")
stxdb.db_write_cmd("update split set dt='2007-07-27' where stk='NFI' and dt='2007-07-26'")
stxdb.db_write_cmd("update split set dt='2006-03-03' where stk='NTLI' and dt='2006-02-03'")
stxdb.db_write_cmd("insert into split values ('OCNFD', '2011-07-05', 20, 0)")
stxdb.db_write_cmd("insert into split values ('OCNW', '2006-03-10', 45.4545, 0)")
stxdb.db_write_cmd("update eod set o=1.15*o, h=1.15*h, l=1.15*l, c=1.15*c, v=v/1.15 where stk='OIIM' and dt='2011-03-02'")
stxdb.db_write_cmd("update split set dt='2012-09-28' where stk='PAA' and dt='2012-10-01'")
stxdb.db_write_cmd("insert into split values ('PACW', '2006-07-07', 100.0, 0)")
stxdb.db_write_cmd("insert into split values ('PBR.A', '2007-06-29', 0.5127, 2)")
stxdb.db_write_cmd("insert into split values ('PBR.A', '2008-05-07', 0.5139, 2)")
stxdb.db_write_cmd("insert into split values ('PC', '2003-05-28', 1.25, 0)")
stxdb.db_write_cmd("insert into split values ('PCH', '2006-02-09', 0.7001, 0)")
stxdb.db_write_cmd("delete from eod where stk='PCX'")
stxdb.db_write_cmd("insert into split values ('PDLI', '2008-05-05', 0.6916, 2)")
stxdb.db_write_cmd("insert into split values ('PFGC', '2001-04-27', 0.5004, 0)")
stxdb.db_write_cmd("insert into split values ('PGF', '2002-05-03', 4.0, 0)")
stxdb.db_write_cmd("insert into split values ('PLUGD', '2011-05-19', 10.0, 0)")
stxdb.db_write_cmd("insert into split values ('PUDA', '2009-08-05', 7.5, 0)")
stxdb.db_write_cmd("insert into split values ('PWAVD', '2011-10-28', 5.75, 0)")
stxdb.db_write_cmd("insert into split values ('RAH', '2012-02-03', 0.8430, 2)")
stxdb.db_write_cmd("insert into split values ('RDA', '2010-11-09', 90.9091, 0)")
stxdb.db_write_cmd("insert into split values ('REXI', '2005-06-30', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('RHA', '2007-06-11', 11.5038, 0)")
stxdb.db_write_cmd("insert into split values ('RNWKD', '2011-08-30', 5.0, 0)")
stxdb.db_write_cmd("insert into split values ('RPTP', '2009-09-29', 14.0000, 0)")
stxdb.db_write_cmd("insert into split values ('SAYCY', '2006-10-17', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('SCMR', '2010-12-22', 0.7517, 0)")
stxdb.db_write_cmd("insert into split values ('SCO', '2001-09-10', 0.7501, 0)")
stxdb.db_write_cmd("delete from eod where stk='SHFL' and dt='2001-04-30'")
stxdb.db_write_cmd("delete from eod where stk='SHOP'")
stxdb.db_write_cmd("delete from eod where stk='SHX'")
stxdb.db_write_cmd("insert into split values ('SKS', '2006-05-01', 0.8081, 0)")
stxdb.db_write_cmd("delete from eod where stk='SSI' and dt between '2005-08-08' and '2005-08-12'")
stxdb.db_write_cmd("delete from eod where stk='STSA' and dt between '2010-11-15' and '2010-11-18'")
stxdb.db_write_cmd("insert into split values ('TELK', '2012-03-30', 25.0, 0)")
stxdb.db_write_cmd("insert into split values ('TIN', '2007-12-07', 0.7960, 0)")
stxdb.db_write_cmd("insert into split values ('TISI', '2007-08-03', 2.0, 0)")
stxdb.db_write_cmd("insert into split values ('TMRK', '2005-05-16', 10.0, 0)")
stxdb.db_write_cmd("delete from eod where stk='TMS' and dt between '2010-02-22' and '2011-04-13'")
stxdb.db_write_cmd("delete from eod where stk='TMTA' and dt between '2007-08-27' and '2007-10-23'")
stxdb.db_write_cmd("insert into split values ('TPC', '2007-10-12', 2.3779, 0)")
stxdb.db_write_cmd("delete from eod where stk='TPCG'")
stxdb.db_write_cmd("insert into split values ('TRIDQ', '2003-12-12', 0.7151, 2)")
stxdb.db_write_cmd("insert into split values ('TRIDQ', '2005-11-18', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('TRIN', '2008-04-16', 2.5, 0)")
stxdb.db_write_cmd("insert into split values ('TW', '2007-07-13', 2.3147, 2)")
stxdb.db_write_cmd("insert into split values ('TXCC', '2005-07-19', 0.8293, 2)")
stxdb.db_write_cmd("insert into split values ('TYC', '2012-09-28', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('TYH', '2009-11-19', 0.8476, 2)")
stxdb.db_write_cmd("insert into split values ('UAM', '2011-04-29', 0.3970, 2)")
stxdb.db_write_cmd("insert into split values ('UCBID', '2004-04-28', 0.667, 0)")
stxdb.db_write_cmd("insert into split values ('UCBID', '2011-06-17', 5.0, 0)")
stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, v=2*v/3 where stk='UOPX' and dt='2001-04-30'")
stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, v=2*v/3 where stk='UOPX' and dt='2001-05-07'")
stxdb.db_write_cmd("insert into split values ('USG', '2006-06-30', 0.7850, 2)")
stxdb.db_write_cmd("insert into split values ('VALE.P', '2004-09-03', 0.333, 0)")
stxdb.db_write_cmd("insert into split values ('VALE.P', '2006-06-06', 0.5, 0)")
stxdb.db_write_cmd("insert into split values ('VALE.P', '2007-09-12', 0.50, 0)")
stxdb.db_write_cmd("update split set dt='2005-12-19' where stk='VISG' and dt='2005-12-16'")
stxdb.db_write_cmd("update eod set v=21705000 where stk='VMW' and dt='2008-01-29'")
stxdb.db_write_cmd("insert into split values ('VOLVY', '2007-05-09', 0.2, 0)")
stxdb.db_write_cmd("delete from eod where stk='VPFG' and dt between '2010-05-28' and '2010-07-06'")
stxdb.db_write_cmd("insert into split values ('VRX', '2010-09-27', 0.4172, 2)")
stxdb.db_write_cmd("insert into split values ('VSGN', '2007-05-14', 10.0, 0)")
stxdb.db_write_cmd("insert into split values ('WAC', '2009-04-23', 40.0, 0)")
stxdb.db_write_cmd("insert into split values ('WCBOD', '2011-05-19', 5.0, 0)")
stxdb.db_write_cmd("insert into split values ('WCRX', '2012-08-28', 0.75, 0)")
stxdb.db_write_cmd("insert into split values ('WEBM', '2012-08-16', 6.0, 0)")
stxdb.db_write_cmd("insert into split values ('WIT', '2001-09-10', 0.75, 0)")
stxdb.db_write_cmd("insert into split values ('WWWW', '2007-07-13', 1.5, 0)")
stxdb.db_write_cmd("insert into split values ('WY', '2010-07-19', 0.3768, 2)")
stxdb.db_write_cmd("insert into split values ('YRCWD', '2010-09-30', 25.0, 0)")
