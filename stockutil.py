import pandas as pd
from pymongo import MongoClient
import FinanceDataReader as fdr
from insider import StockInsider


class StockManage:
    def __init__(self):
        self.stockcode = "005930"
        self.mc = MongoClient("mongodb://192.168.10.199:27017/")
        self.mcdb = self.mc['test']
        self.mccolcomp = self.mcdb['stockcomp']
        self.mccoldata = self.mcdb['stockdata']
        self.baseday = '2019-01-01'

    def addAllCompdata2Mongo(self, df):
        self.mccolcomp.insert_many(df.to_dict('records'))

    def getSymbols(self):
        return self.mccolcomp.distinct("Symbol")

    def getStockDataFromFDR(self, scode=None):
        if scode is not None:
            self.stockcode = scode
        self.df_stock = fdr.DataReader(self.stockcode, self.baseday)
        self.df_stock["day"] = self.df_stock.index
        self.df_stock.columns = ["open", "high", "low", "close", "volumn", "price_change", "day"]
        return self.df_stock

    def getLastDay(self, scode):
        lday= self.mccoldata.find({"stockcode": scode}, {"_id": 0, "day": 1}).sort([('day', -1)]).limit(1)
        return list(lday)[0]['day']

    def addStockdata2Mongo(self, lday=None):
        self.df_stock["stockcode"] = self.stockcode
        if lday is None:
            self.mccoldata.insert_many(self.df_stock.to_dict('records'))
        else:
            df = self.df_stock[(self.df_stock['day'] > lastday)]
            if df.shape[0] > 0:
                self.mccoldata.insert_many(df.to_dict('records'))


    def add_ma(self):
        self.df_stock["ma5"] = self.df_stock["close"].rolling(5).mean()
        self.df_stock["ma10"] = self.df_stock["close"].rolling(10).mean()
        self.df_stock["ma20"] = self.df_stock["close"].rolling(20).mean()
        self.df_stock["v_ma5"] = self.df_stock["volumn"].rolling(5).mean()
        self.df_stock["v_ma10"] = self.df_stock["volumn"].rolling(10).mean()
        self.df_stock["v_ma20"] = self.df_stock["volumn"].rolling(20).mean()

    def add_macd(self, df_si):
        self.df_stock["macd_diff"] = df_si["diff"]
        self.df_stock["macd_dea"] = df_si["dea"]
        self.df_stock["macd"] = df_si["macd"]

    def add_rsi(self, df_si):
        self.df_stock["rsi_shift_diff"] = df_si["shift_diff"]
        self.df_stock["rsi_shift_diff_abs"] = df_si["shift_diff_abs"]
        self.df_stock["rsi"] = df_si["rsi"]

    def add_dmi(self, df_si):
        self.df_stock["dmi_up"] = df_si["up"]
        self.df_stock["dmi_down"] = df_si["down"]
        self.df_stock["dmi_pdi"] = df_si["pdi"]
        self.df_stock["dmi_mdi"] = df_si["mdi"]
        self.df_stock["dmi_atr"] = df_si["atr"]
        self.df_stock["dmi_adx"] = df_si["adx"]
        self.df_stock["dmi_adxr"] = df_si["adxr"]

    def add_boll(self, df_si):
        self.df_stock["boll_middle"] = df_si["middle"]
        self.df_stock["boll_up"] = df_si["up"]
        self.df_stock["boll_down"] = df_si["down"]

    def add_env(self, df_si):
        self.df_stock["env_up"] = df_si["up"]
        self.df_stock["env_down"] = df_si["down"]

    def add_obv(self, df_si):
        self.df_stock["obv_close_diff"] = df_si["close_diff"]
        self.df_stock["obv_v"] = df_si["v"]
        self.df_stock["obv"] = df_si["obv"]

    def setdata(self, scode):
        self.stockcode = scode
        pass


# for first data insert to mongodb. get data from local postgresql
def getpgcompinfo():
    import psycopg2
    constr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
    # constr = "host='192.168.10.199' dbname='testdb' user='testuser' password='testpwd'"
    con = psycopg2.connect(constr)
    cursor = con.cursor()
    sql = 'select  "Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region" from "stockcomp" where Length("Symbol")=6'
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.close()
    df_compinfo = pd.DataFrame(result)
    df_compinfo.columns = ["Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth",
                           "Representative", "HomePage", "Region"]
    df_compinfo.fillna("-", inplace=True)
    return df_compinfo


a = StockManage()
# for first data collect
# df_comp = getpgcompinfo()
# a.addAllCompdata2Mongo(df_comp)

for symbol in a.getSymbols():
    df_data = a.getStockDataFromFDR(symbol)
    si = StockInsider(code=symbol, df=df_data)
    a.add_ma()
    a.add_rsi(si.rsi())
    a.add_macd(si.macd())
    a.add_boll(si.boll())
    a.add_obv(si.obv())
    a.add_env(si.env())
    a.add_dmi(si.dmi())

    # for first data collect
    # a.addStockdata2Mongo()
    # print(symbol, df_data.shape[0], 'inserted')

    lastday = a.getLastDay(symbol)
    a.addStockdata2Mongo(lastday)
    print(symbol, " processed")