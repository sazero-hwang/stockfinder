import pandas as pd
from pymongo import MongoClient

mc = MongoClient("mongodb://192.168.10.199:27017/")
mcdb = mc['test']
mccoldata = mcdb['stockdata']

sdata = mccoldata.find({"stockcode": "005930"}, {"_id": 0})
df = pd.DataFrame(sdata)
print(df)
