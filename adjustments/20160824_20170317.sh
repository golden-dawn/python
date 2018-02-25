#!/bin/bash
psql -d stx -c "delete from eods where stk='ARWA'"
psql -d stx -c "insert into dividends values ('BAS', '2016-12-23', 86.6667, 0)"
psql -d stx -c "insert into dividends values ('CDOR', '2017-03-15', 6.5359, 0)"
psql -d stx -c "insert into dividends values ('DGAZ', '2017-03-15', 5.0, 0)"
psql -d stx -c "insert into dividends values ('DRYS', '2016-10-31', 15, 0)"
psql -d stx -c "insert into dividends values ('DSU', '2016-11-15', 3.0, 0)"
psql -d stx -c "insert into dividends values ('DXJR', '2016-12-27', 0.5304, 2)"
psql -d stx -c "insert into dividends values ('EES', '2017-02-03', 0.3333, 0)"
psql -d stx -c "insert into dividends values ('EMD', '2016-12-16', 1.3764, 0)"
psql -d stx -c "insert into dividends values ('ERUS', '2016-11-04', 2.0, 0)"
psql -d stx -c "insert into dividends values ('EZM', '2017-02-03', 0.3333, 0)"
psql -d stx -c "insert into dividends values ('FFWM', '2017-01-18', 0.50, 0)"
psql -d stx -c "insert into dividends values ('FINU', '2017-01-11', 0.5, 0)"
psql -d stx -c "insert into dividends values ('FINZ', '2017-01-11', 2.0, 0)"
psql -d stx -c "insert into dividends values ('GNVC', '2016-11-30', 10.000, 0)"
psql -d stx -c "insert into dividends values ('GOEX', '2016-12-27', 0.7034, 2)"
psql -d stx -c "insert into dividends values ('HEWI', '2016-12-01', 0.7508, 2)"
psql -d stx -c "insert into dividends values ('HGJP', '2016-12-20', 0.8382, 2)"
psql -d stx -c "insert into dividends values ('HTM', '2016-11-09', 5.8333, 0)"
psql -d stx -c "insert into dividends values ('IDXG', '2016-12-28',10, 0)"
psql -d stx -c "insert into dividends values ('KBSF', '2017-02-08', 15, 0)"
psql -d stx -c "insert into dividends values ('KRU', '2017-01-11', 0.5, 0)"
psql -d stx -c "insert into dividends values ('LTL', '2017-01-11', 0.5, 0)"
psql -d stx -c "insert into dividends values ('MDGS', '2017-03-14', 10, 0)"
psql -d stx -c "insert into dividends values ('NGE', '2017-03-15', 4.0, 0)"
psql -d stx -c "insert into dividends values ('PBIB', '2016-12-15', 5.0, 0)"
psql -d stx -c "insert into dividends values ('RGCO', '2017-02-28', 0.7032, 2)"
psql -d stx -c "insert into dividends values ('RGSE', '2017-01-25', 30, 0)"
psql -d stx -c "insert into dividends values ('ROSG', '2017-03-16', 10.00, 0)"
psql -d stx -c "insert into dividends values ('SFBS', '2016-12-20', 0.5000, 0)"
psql -d stx -c "insert into dividends values ('SGNL', '2016-11-04', 30, 0)"
psql -d stx -c "insert into dividends values ('TEUM', '2017-02-24', 25, 0)"
psql -d stx -c "insert into dividends values ('TLK', '2016-10-24', 0.5, 0)"
psql -d stx -c "insert into dividends values ('TVIX', '2017-03-15', 10, 0)"
psql -d stx -c "insert into dividends values ('TVIZ', '2017-03-15', 5, 0)"
psql -d stx -c "insert into dividends values ('UCC', '2017-01-11', 0.50, 0)"
psql -d stx -c "insert into dividends values ('VIIX', '2017-03-15', 5, 0)"
psql -d stx -c "insert into dividends values ('VMIN', '2016-12-28', 0.5, 0)"
psql -d stx -c "insert into dividends values ('WGBS', '2016-11-28', 5, 0)"
psql -d stx -c "insert into dividends values ('XBKS', '2016-12-12', 10, 0)"
psql -d stx -c "delete from eods where stk='ZJZZT'"
psql -d stx -c "delete from eods where stk='ZWZZT'"
psql -d stx -c "delete from eods where stk='ZXZZT'"
psql -d stx -c "insert into dividends values ('EVHC', '2016-12-01', 3, 0)"
psql -d stx -c "update dividends set date='2016-08-23' where stk='BRZU' and date='2016-08-24'"