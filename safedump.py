import mysql.connector
import time
import json
import datetime
import decimal
import getpass
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--user', '-u', type=str)
parser.add_argument('--host', '-ho', type=str, default='127.0.0.1')
parser.add_argument('--database', '-db', type=str)
parser.add_argument('--password', '-p', action='store_true')
parser.add_argument('--output', '-o', type=str, default='safedump_output.json')
ARGS = parser.parse_args()

if ARGS.password:
    PASSWORD = getpass.getpass()
else:
    PASSWORD = None

def make_conn():
    conn = mysql.connector.connect(user=ARGS.user, password=PASSWORD,
        host=ARGS.host, database=ARGS.database)
    return conn

def adjust_row(row):
    res = []
    for r in row:
        if isinstance(r, datetime.datetime):
            r = { 'is_datetime': True, 'value': r.isoformat() }
        elif isinstance(r, decimal.Decimal):
            r = { 'is_decimal': True, 'value': r.as_tuple() }
        res.append(r)
    return res

def dump_table(t, outfileobj):
    print(f'Processing: {t} ...')
    conn = make_conn()
    cur = conn.cursor()
    cur.execute(f'SHOW CREATE TABLE {t}')
    create_stmt = cur.fetchone()[-1]
    cur.execute(f'SELECT * FROM {t} LIMIT 1')
    _ = cur.fetchone()
    col_names = list(cur.column_names)
    json.dump({ 'table_name': t, 'create_stmt': create_stmt, 'col_names': col_names }, outfileobj)
    # json.dump(col_names, outfileobj)
    outfileobj.write('\n')
    id_column = col_names[0]
    print('ID column:', id_column)
    cur.execute(f'SELECT COUNT(*) FROM {t}')
    count = cur.fetchone()[0]
    print(f'Count OK: {count}')
    for i in range(count):
        try:
            cur.execute(f'SELECT * FROM {t} ORDER BY {id_column} ASC LIMIT 1 OFFSET {i}')
            row = cur.fetchone()
        except mysql.connector.Error:
            print(f'Error retrieving row {i}')
            time.sleep(1)
            return
            conn = make_conn()
            cur = conn.cursor()
        row = adjust_row(row)
        json.dump(row, outfileobj)
        outfileobj.write('\n')

conn = make_conn()
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
tables = [ e[0] for e in cursor ]

outfileobj = open(ARGS.output, 'w')
for t in tables:
    dump_table(t, outfileobj)
