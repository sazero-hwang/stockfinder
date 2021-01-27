import pandas as pd
from pymongo import MongoClient
import FinanceDataReader as fdr
from insider import StockInsider


def compinfo():
    import psycopg2
    constr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
    con = psycopg2.connect(constr)
    cursor = con.cursor()
    sql = 'select  "Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region" from "stockcomp" where Length("Symbol")=6'
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return pd.DataFrame(result)


def make_ma(df):
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["v_ma5"] = df["volumn"].rolling(5).mean()
    df["v_ma10"] = df["volumn"].rolling(10).mean()
    df["v_ma20"] = df["volumn"].rolling(20).mean()


def make_macd(df, df_si):
    df["macd_diff"] = df_si["diff"]
    df["macd_dea"] = df_si["dea"]
    df["macd"] = df_si["macd"]


def make_rsi(df, df_si):
    df["rsi_shift_diff"] = df_si["shift_diff"]
    df["rsi_shift_diff_abs"] = df_si["shift_diff_abs"]
    df["rsi"] = df_si["rsi"]


def make_dmi(df, df_si):
    df["dmi_up"] = df_si["up"]
    df["dmi_down"] = df_si["down"]
    df["dmi_pdi"] = df_si["pdi"]
    df["dmi_mdi"] = df_si["mdi"]
    df["dmi_atr"] = df_si["atr"]
    df["dmi_adx"] = df_si["adx"]
    df["dmi_adxr"] = df_si["adxr"]


def make_boll(df, df_si):
    df["boll_middle"] = df_si["middle"]
    df["boll_up"] = df_si["up"]
    df["boll_down"] = df_si["down"]


def make_env(df, df_si):
    df["env_up"] = df_si["up"]
    df["env_down"] = df_si["down"]


def make_obv(df, df_si):
    df["obv_close_diff"] = df_si["close_diff"]
    df["obv_v"] = df_si["v"]
    df["obv"] = df_si["obv"]


df_comp = compinfo()
df_comp.columns = ["Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region"]
df_comp.fillna("-", inplace=True)
# print(df_comp)

mc = MongoClient("mongodb://localhost:27017/")
mcdb = mc['test']
colcomp = mcdb['stockcomp']
coldata = mcdb['stockdata']
colcomp.insert_many(df_comp.to_dict('records'))

for scode in df_comp["Symbol"]:
    print(scode)
    df_stock = fdr.DataReader(scode, '2018-01-01')
    df_stock["day"] = df_stock.index
    df_stock.columns = ["open", "high", "low", "close", "volumn", "price_change", "day"]
    make_ma(df_stock)
    si = StockInsider(code=scode, df=df_stock)
    df_stock["stockcode"] = scode

    make_macd(df_stock, si.macd())
    make_rsi(df_stock, si.rsi())
    make_dmi(df_stock, si.dmi())
    make_boll(df_stock, si.boll())
    make_env(df_stock, si.env())
    make_obv(df_stock, si.obv())

    coldata.insert_many(df_stock.to_dict('records'))
    # print(df_stock.tail())
    # break

