import pymysql.cursors
import pandas as pd
from datetime import datetime, timedelta
import FinanceDataReader as fdr


def getdbdata(cur, scode):
    sql = f"SELECT `date`,`open`,`high`,`low`,`close`,`volumn`,`change` FROM `stockdata` WHERE symbol='{scode}' ORDER BY date DESC LIMIT 20"
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)


def getlastday(cur, scode):
    sql = f"SELECT `date` FROM `stockdata` WHERE symbol='{scode}' ORDER BY date DESC LIMIT 1"
    cur.execute(sql)
    result = cur.fetchone()
    if(result == None):
        return None
    return result['date']


def getdbcodes(cur):
    sql = "SELECT `symbol` FROM `company`"
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)


conn = pymysql.connect(host='localhost', user='root', db='test',
                       charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
dfcodes = getdbcodes(cursor)
for idx, row in dfcodes.iterrows():
    lastday = getlastday(cursor, row['symbol'])
    if(lastday == None):
        print(idx, lastday)
        continue
    nextday = lastday + timedelta(days=1)
    if(nextday.strftime("%Y-%m-%d") > datetime.today().strftime('%Y-%m-%d')):
        print(idx, nextday, 'will fetch data')
        continue
    dfnew = fdr.DataReader(row['symbol'], nextday.strftime("%Y-%m-%d"), datetime.today().strftime('%Y-%m-%d'))
    if(len(dfnew) == 0):
        print(idx, 'No New data')
        continue
    dfnew['date'] = dfnew.index
    for idx2, row2 in dfnew.iterrows():
        df = getdbdata(cursor, row['symbol'])
        if(len(df) == 0):
            continue
        ma5 = (df.loc[0:3, 'close'].sum() + row2['Close']) / 5
        ma10 = (df.loc[0:8, 'close'].sum() + row2['Close']) / 10
        ma20 = (df.loc[:, 'close'].sum() + row2['Close']) / 20
        v_ma5 = (df.loc[0:3, 'volumn'].sum() + row2['Volume']) / 5
        v_ma10 = (df.loc[0:8, 'volumn'].sum() + row2['Volume']) / 10
        v_ma20 = (df.loc[:, 'volumn'].sum() + row2['Volume']) / 20
        print(idx, row2['date'], ma5, ma10, ma20, v_ma5, v_ma10, v_ma20)
        sqlins = 'insert into stockdata values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.execute(sqlins, (row2['date'], row2['Open'], row2['High'], row2['Low'], row2['Close'],
                                row2['Volume'], row2['Change'], row['symbol'], ma5, ma10, ma20, v_ma5, v_ma10, v_ma20))

cursor.close()
conn.close()
