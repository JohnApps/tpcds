# tpcds_proto.py
#
import duckdb

conn = duckdb.connect()
cur = conn.cursor()
tpc = cur.execute("INSTALL tpcds;")
tpc = cur.execute("load tpcds;")
sec = cur.execute("CREATE SECRET (TYPE quack, TOKEN 'super_secret');");
loc = cur.execute("attach 'quack.localhost' as remote \
                    (type quack, DISABLE_SSL true);")
query = "select count(*) from remote.customer"

res = cur.execute(query).fetchone()

for x in res:
    print(x)

