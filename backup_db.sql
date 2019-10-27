\copy (select * from eods where dt > :last_dt) to /tmp/eods.csv with CSV;
\copy (select * from dividends where dt>:'last_dt') to /tmp/divis.csv with CSV;
\copy (select * from options where expiry in (:'exp1', :'exp2', :'exp3', :'exp4') and  dt>:'last_dt') to /tmp/opts.csv with CSV;
\copy (select * from leaders) to /tmp/ldrs.csv with CSV;
\copy (select * from setups) to /tmp/setups.csv with CSV;

