import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import FinanceDataReader as fdr


def getdbdata(cur, scode):
    sql = f'SELECT "Date","Open","High","Low","Close","Volume","Change" FROM "stockdata" WHERE "Symbol"=\'{scode}\' ORDER BY "Date" DESC LIMIT 20'
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)


def getlastday(cur, scode):
    sql = f'SELECT "Date" FROM "stockdata" WHERE "Symbol"=\'{scode}\' ORDER BY "Date" DESC LIMIT 1'
    cur.execute(sql)
    if(cur.rowcount == 0):
        return None
    result = cur.fetchone()
    # if(result == None):
    #     return None
    return result


def getdbcodes(cur):
    sql = 'SELECT "Symbol" FROM "stockcomp" where LENGTH("Symbol") = 6'
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)


connstr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
conn = psycopg2.connect(connstr)
conn.autocommit = True
cursor = conn.cursor()

dfcodes = getdbcodes(cursor)

for idx, row in dfcodes.iterrows():
    lastday = getlastday(cursor, row[0])
    if(lastday == None):
        print(idx, lastday)
        continue
    nextday = lastday[0] + timedelta(days=1)
    if(nextday.strftime("%Y-%m-%d") > datetime.today().strftime('%Y-%m-%d')):
        print(idx, nextday, 'will fetch data')
        continue
    dfnew = fdr.DataReader(row[0], nextday.strftime("%Y-%m-%d"), datetime.today().strftime('%Y-%m-%d'))
    if(len(dfnew) == 0):
        print(idx, 'No New data')
        continue
    dfnew['date'] = dfnew.index
    for idx2, row2 in dfnew.iterrows():
        df = getdbdata(cursor, row[0])
        if(len(df) == 0):
            continue
        ma5 = (df.loc[0:3, 4].sum() + row2['Close']) / 5
        ma10 = (df.loc[0:8, 4].sum() + row2['Close']) / 10
        ma20 = (df.loc[:, 4].sum() + row2['Close']) / 20
        v_ma5 = (df.loc[0:3, 5].sum() + row2['Volume']) / 5
        v_ma10 = (df.loc[0:8, 5].sum() + row2['Volume']) / 10
        v_ma20 = (df.loc[:, 5].sum() + row2['Volume']) / 20
        print(idx, row2['date'], ma5, ma10, ma20, v_ma5, v_ma10, v_ma20)
        sqlins = 'insert into "stockdata" values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.execute(sqlins, (row2['date'], row2['Open'], row2['High'], row2['Low'], row2['Close'],
                                row2['Volume'], row2['Change'], row[0], ma5, ma10, ma20, v_ma5, v_ma10, v_ma20))

cursor.close()
conn.close()
