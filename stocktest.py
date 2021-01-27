from pymongo import MongoClient
import pandas as pd
from insider import StockInsider
import FinanceDataReader as fdr

# def compinfo():
#     import psycopg2
#     constr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
#     con = psycopg2.connect(constr)
#     cur = con.cursor()
#     sql = 'select  "Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region" from "stockcomp"'
#     cur.execute(sql)
#     result = cur.fetchall()
#     return pd.DataFrame(result)
#
#
#
# df = compinfo()
# df.columns = ["Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region"]
# df.fillna("-", inplace=True)
# print(df.to_dict('records'))

def stockinfo(scode):
    import psycopg2
    constr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
    con = psycopg2.connect(constr)
    cur = con.cursor()
    sql = f'select  "Date", "High", "Low", "Open", "Close", "Volume", "Change", "ma5", "ma10", "ma20", "v_ma5", "v_ma10", "v_ma20" from "stockdata" WHERE "Symbol"=\'{scode}\''
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)

# ["day", "high", "low", "open", "close", "volumn"]
df = fdr.DataReader('005930','2020-01-01')
df["day"] = df.index
df.columns = ['open', 'high', 'low', 'close', 'volumn', "percent_change", "day"]
# print(df)
# df_si = pd.DataFrame()
#
# df_si["high"] = df["High"]
# df_si["day"] = df.index
# df_si["close"] = df["Close"]
# df_si["price_change"] = df["Close"].diff()
# print(df_si)
# df_si["low"] = df["Low"].values
# df_si["open"] = df["Open"].values
# df_si["close"] = df["Close"].values
# df_si["volumn"] = df["Volume"].values
# df_si["percent_change"] = df["Change"].values
# df_si["ma5"] = df_si["close"].rolling(5).mean()
# df_si["ma10"] = df_si["close"].rolling(10).mean()
# df_si["ma20"] = df_si["close"].rolling(20).mean()
# df_si["v_ma5"] = df_si["volumn"].rolling(5).mean()
# df_si["v_ma10"] = df_si["volumn"].rolling(10).mean()
# df_si["v_ma20"] = df_si["volumn"].rolling(20).mean()
#
# si = StockInsider(code='005930', df=df_si)
#
# macd = si.macd()
# df_si["macd_diff"] = macd["diff"]
# df_si["macd_dea"] = macd["dea"]
# df_si["macd"] = macd["macd"]
#
# rsi = si.rsi()
# df_si["rsi_shift_diff"] = rsi["shift_diff"]
# df_si["rsi_shift_diff_abs"] = rsi["shift_diff_abs"]
# df_si["rsi"] = rsi["rsi"]
#
# mi = si.mi()
# df_si["mi"] = mi["mi"]
#
# kdj = si.kdj()
# df_si["kdj_k"] = kdj["K"]
# df_si["kdj_d"] = kdj["D"]
# df_si["kdj_j"] = kdj["J"]
# # print(df_si)
# df_si["stockcode"] = '005930'

# df = stockinfo('005930')
# print(df)
# df['Symbol'] = '005930'
df["ma5"] = df["close"].rolling(5).mean()
df["ma10"] = df["close"].rolling(10).mean()
df["ma20"] = df["close"].rolling(20).mean()
df["v_ma5"] = df["volumn"].rolling(5).mean()
df["v_ma10"] = df["volumn"].rolling(10).mean()
df["v_ma20"] = df["volumn"].rolling(20).mean()
# print(df)
si = StockInsider(code='005930', df=df)
print(si.macd())

# df = stockinfo('005930')
# df.columns = ["day", "high", "low", "open", "close", "volumn", "percent_change", "ma5", "ma10", "ma20", "v_ma5", "v_ma10", "v_ma20"]
# df.fillna("-", inplace=True)
# print(df)
# si = StockInsider(code='005930', df=df)
# si.plot_bbiboll()



# myc = MongoClient("mongodb://localhost:27017/")
# mydb = myc['test']
# mycol = mydb['stockdata']
# mycol.insert_many(df_si.to_dict('records'))


# insert
# x = mycol.insert_one({"name":"Hwang", "address":"Gimpo Janggi"})
# print(x.inserted_id)

# select
# a = mycol.find({})
# for b in a:
#     print(b)