\copy (select * from eods where dt > 'LAST_DT') to /tmp/eods.csv with CSV;
\copy (select * from dividends where dt > 'LAST_DT') to /tmp/divis.csv with CSV;
\copy (select * from options where expiry in ('EXP1', 'EXP2', 'EXP3', 'EXP4') and  dt>'LAST_DT') to /tmp/opts.csv with CSV;
\copy (select * from leaders) to /tmp/ldrs.csv with CSV;
\copy (select * from setups) to /tmp/setups.csv with CSV;

