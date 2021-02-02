import pandas as pd
from pymongo import MongoClient
import FinanceDataReader as fdr
from insider import StockInsider
from datetime import datetime, timedelta

mc = MongoClient("mongodb://localhost:27017/")
mcdb = mc['test']
colcomp = mcdb['stockcomp']
coldata = mcdb['stockdata']
import numpy as np
symbols = colcomp.distinct("Symbol")
for code in symbols:
    print(code + " processing...")
    colm = {"_id": 0, "open": 1, "high": 1, "low": 1, "close": 1, "volumn": 1, "price_change": 1, "day": 1}
    stockdata = coldata.find({"stockcode": code}, colm)
    df = pd.DataFrame(list(stockdata))
    lastday = df.tail(1)["day"].values[0]
    # nextday = lastday + timedelta(days=1)
    # nextday = lastday + np.timedelta64(1, 'D')
    # print(pd.to_datetime(nextday).date())
    # if pd.to_datetime(nextday).date() > pd.to_datetime(datetime.today()).date():
    #     print(nextday,  'will fetch data')
    #     continue
    # dfnew = fdr.DataReader(code, pd.to_datetime(nextday).date())
    # dfnew["day"] = dfnew.index
    # dfnew.columns = ["open", "high", "low", "close", "volumn", "price_change", "day"]
    # df.append(dfnew.iloc[1:], ignore_index=True)
    # si = StockInsider(code=code, df=df)
    # print(df.tail(1)["ma5"].values[0])
    # print((df.iloc[-4:]["close"].sum() + dfnew["close"].values[0])/5)
    print(df)
    # print(dfnew)
    break
