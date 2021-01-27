import FinanceDataReader as fdr

import pymysql
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
import MySQLdb

# engine = create_engine("mysql+mysqldb://root:"+"password"+"@localhost/db_name", encoding='utf-8')
engine = create_engine("mysql+mysqldb://root" + "@localhost/test", encoding='utf-8')
conn = engine.connect()

qry = ("DROP TABLE IF EXISTS `company`; CREATE TABLE `company` ( "
       "`index` bigint(20) DEFAULT NULL, "
       "`symbol` varchar(10) DEFAULT NULL, "
       "`market` text DEFAULT NULL, "
       "`name` text DEFAULT NULL, "
       "`sector` text DEFAULT NULL, "
       "`industry` text DEFAULT NULL, "
       "`listingDate` datetime DEFAULT NULL, "
       "`settleMonth` text DEFAULT NULL, "
       "`representative` text DEFAULT NULL, "
       "`homePage` text DEFAULT NULL, "
       "`region` text DEFAULT NULL, "
       "KEY `ix_company_index` (`index`), "
       "KEY `ix_company_symbol` (`symbol`(10)) "
       ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ")
conn.execute(qry)

qry = ("DROP TABLE IF EXISTS `stockdata`; CREATE TABLE `stockdata` ( "
       "`date` datetime DEFAULT NULL, "
       "`open` bigint(20) DEFAULT NULL, "
       "`high` bigint(20) DEFAULT NULL, "
       "`low` bigint(20) DEFAULT NULL, "
       "`close` bigint(20) DEFAULT NULL, "
       "`volumn` bigint(20) DEFAULT NULL, "
       "`change` double DEFAULT NULL, "
       "`symbol` varchar(10) DEFAULT NULL, "
       "`ma5` double DEFAULT NULL, "
       "`ma10` double DEFAULT NULL, "
       "`ma20` double DEFAULT NULL, "
       "`v_ma5` double DEFAULT NULL, "
       "`v_ma10` double DEFAULT NULL, "
       "`v_ma20` double DEFAULT NULL, "
       "KEY `ix_stockdata_Date` (`date`), "
       "KEY `ix_stockdata_symbol` (`symbol`(10)) "
       ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ")
conn.execute(qry)

df = fdr.StockListing('KRX')
df.to_sql(con=conn, name='company', if_exists='append')

for index, row in df.iterrows():
    print(index, row['Symbol'], row['Name'])
    df_comp = fdr.DataReader(row['Symbol'])
    if len(df_comp) == 0:
        continue
    df_comp['symbol'] = row['Symbol']
    df_comp.columns = ['open', 'high', 'low', 'close', 'volumn', "change", 'symbol']
    df_comp["ma5"] = df_comp["close"].rolling(5).mean()
    df_comp["ma10"] = df_comp["close"].rolling(10).mean()
    df_comp["ma20"] = df_comp["close"].rolling(20).mean()
    df_comp["v_ma5"] = df_comp["volumn"].rolling(5).mean()
    df_comp["v_ma10"] = df_comp["volumn"].rolling(10).mean()
    df_comp["v_ma20"] = df_comp["volumn"].rolling(20).mean()
    df_comp.to_sql(con=conn, name='stockdata', if_exists='append')
    print(f'{len(df_comp)} inserted')

conn.close()
