import requests
import json
import datetime
import sqlite3
import db_helper
import pandas
from pandas import json_normalize

url = "https://www.quandl.com/api/v3/datatables/SHARADAR/SFP.json?api_key=sCJNpBm_3CDYKxgRbMFS"
cnt = requests.get(url).content

ds = json.loads(cnt)

# json_vw = pandas.DataFrame(df)
# print(json_vw.to_json(indent=2))

df = list()

for row in ds['datatable']['data']:
    date_a = datetime.datetime.strptime(row[1], '%Y-%m-%d')
    date_b = datetime.datetime.strptime(row[9], '%Y-%m-%d')
    payload = (str(row[0]), date_a, float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6]),
               float(row[7]), float(row[8]), date_b)
    df.append(payload)

################################
def create_db_table(db_name):
    sql_statement = '''create table stocks(ticker varchar(50), date datetime, open float, high float,
    low float, close float, volume float, dividends float, closeunadj float, lastupdated datetime)'''
    try:
        db_cxn = sqlite3.connect(db_name)
        cursor = db_cxn.cursor()
        cursor.execute(sql_statement)

    except Exception as e:
        return 'Error:', e
    else:
        return 'Table created'


ice = create_db_table('Employee')

print(ice)

#################################

def load_data_into_db(db_name):
    try:
        db_cxn = sqlite3.connect(db_name)
        cursor = db_cxn.cursor()
        cursor.executemany('insert into stocks values(?,?,?,?,?,?,?,?,?,?)', df)
    except Exception as e:
        return 'Error:', e
    else:
        db_cxn.commit()
        return 'Data inserted'


ice2 = load_data_into_db('Employee')

print(ice2)
