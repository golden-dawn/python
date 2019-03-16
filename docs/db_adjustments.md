In Python, retrieve some data from the database (for `ABC`), and then
replace with a new ticker (`XXXX`):

```
>>> from psycopg2 import sql
>>> q = sql.Composed([sql.SQL('select * from eods where stk='), sql.Literal('ABC')])
>>> xxxx_data = [['XXXX'] + list(x[1:]) for x in res[-10:]]
```

In the database, create a new ticker (`XXXX`) in the `equities` table:
```
stx_ng=# insert into equities values('XXXX', 'Test ticker', 'US_Stocks', 'US');
INSERT 0 1
```

Back to the python code, insert the values for the ticker`XXXX` in the
database:

```
>>> with cnx.cursor() as crs:
...     for x in xxxx_data:
...             crs.execute('insert into eods values' + crs.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s)', x))
... 
```

Modify the list of values (change the open, high, low, close and
volume), and insert the new values in the database:

```
>>> xxxx_data_1 = [[x[0], x[1], float(x[2]) + 100.0,  float(x[3]) + 100, float(x[4]) + 100, float(x[5]) + 100, x[6] * 10, x[7]] for x in xxxx_data]
>>> with cnx.cursor() as crs:
...     for x in xxxx_data_1:
...             crs.execute('insert into eods values' + crs.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s)', x) + ' on conflict on constraint eods_pkey do update set o=excluded.o, hi=excluded.hi, lo=excluded.lo, c=excluded.c, volume=excluded.volume, open_interest=excluded.open_interest')
... 
```