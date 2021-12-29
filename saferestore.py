import mysql.connector
import time
import json
import datetime
import sys
import decimal
import getpass
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--user', '-u', type=str)
parser.add_argument('--host', '-ho', type=str, default='127.0.0.1')
parser.add_argument('--database', '-db', type=str)
parser.add_argument('--password', '-p', action='store_true')
parser.add_argument('--input', '-i', type=str, default='safedump_output.json')
ARGS = parser.parse_args()

if ARGS.password:
    PASSWORD = getpass.getpass()
else:
    PASSWORD = None

def make_conn():
    conn = mysql.connector.connect(user=ARGS.user, password=PASSWORD,
        host=ARGS.host, database=ARGS.database)
    return conn

def start_table(data, conn):
    print('Processing', data['table_name'], '...')
    cur = conn.cursor()
    stmt = 'SET FOREIGN_KEY_CHECKS=0'
    print(stmt)
    cur.execute(stmt)
    _ = cur.fetchone()
    stmt = f'DROP TABLE IF EXISTS {data["table_name"]}'
    print(stmt)
    cur.execute(stmt)
    _ = cur.fetchone()
    stmt = data['create_stmt']
    print(stmt)
    cur.execute(stmt)
    _ = cur.fetchone()
    cur = conn.cursor(prepared=True)
    stmt = 'SET FOREIGN_KEY_CHECKS=0'
    print(stmt)
    cur.execute(stmt)
    _ = cur.fetchone()
    # stmt = f'INSERT INTO {data["table_name"]}({ ",".join(data["col_names"])  }) VALUES({ ",".join( [ "COALESCE(?, DEFAULT(" + data["col_names"][i] + "))" for i in range(len(data["col_names"])) ] ) })'
    print(stmt)
    return (data, cur) # , stmt)

def adjust_row(row):
    res = []
    for r in row:
        if isinstance(r, dict) and 'is_datetime' in r and r['is_datetime']:
            r = datetime.datetime.fromisoformat(r['value'])
        elif isinstance(r, dict) and 'is_decimal' in r and r['is_decimal']:
            r = decimal.Decimal(r['value'])
        res.append(r)
    return res

def add_row(t, row, conn):
    tbl_data, cur = t
    row = adjust_row(row)
    col_names = tbl_data["col_names"]
    col_names = [ col_names[i] for i in range(len(col_names)) if row[i] is not None ]
    row = [ r for r in row if r is not None ]
    stmt = f'INSERT INTO {tbl_data["table_name"]}({ ",".join(col_names) }) VALUES({ ",".join( ["?"] * len(row) )  })'
    cur.execute(stmt, row)
    _ = cur.fetchone()


conn = make_conn()
infileobj = open(ARGS.input)
while (line := infileobj.readline()):
    try:
        data = json.loads(line)
    except:
        print('Error parsing:', line)
        sys.exit(-1)
    if isinstance(data, dict):
        t = start_table(data, conn)
        row_count = 0
    elif isinstance(data, list):
        if row_count and row_count % 100 == 0:
            print(row_count)
        add_row(t, data, conn)
        row_count += 1
    else:
        raise TypeError('Unexpected type')
