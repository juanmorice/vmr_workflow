"""Check if UPC table exists."""
import psycopg2
import os

conn = psycopg2.connect(
    user=os.getenv('YELLOWBRICK_USER'),
    password=os.getenv('YELLOWBRICK_PASSWORD'),
    host='orlpybvip01.catmktg.com',
    port='5432',
    database='py1usta1'
)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM vmr_unilever_upclmc_laura_calderon")
print(f'Rows in table: {cur.fetchone()[0]}')
cur.execute("SELECT * FROM vmr_unilever_upclmc_laura_calderon LIMIT 3")
rows = cur.fetchall()
print(f'Sample rows: {rows}')
cur.close()
conn.close()
