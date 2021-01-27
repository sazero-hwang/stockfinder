import pandas as pd
from pymongo import MongoClient

mc = MongoClient("mongodb://localhost:27017/")
mcdb = mc['test']
mycol = mcdb['stockcomp']

symbols = mycol.distinct("Symbol")

for code in symbols:
    print(code)

