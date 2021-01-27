import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import FinanceDataReader as fdr
import plotly.graph_objects as go
from insider import StockInsider

def getdbdata(cur, scode):
    # sql = f'SELECT "Date","Open","High","Low","Close","Volume","Change" FROM "stockdata" WHERE "Symbol"=\'{scode}\' ORDER BY "Date" DESC LIMIT 20'
    sql = f'SELECT "Date", "High", "Low", "Open", "Close", "Volume" FROM "stockdata" WHERE "Symbol"=\'{scode}\' and "Date" >= \'2020-01-01\''
    #"day", "high", "low", "open", "close", "volumn"
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)

connstr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
conn = psycopg2.connect(connstr)
conn.autocommit = True
cursor = conn.cursor()

stockcode = '005930'

df = getdbdata(cursor, stockcode)
#df.columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Change", "Symbol", "ma5", "ma10", "ma20", "v_ma5", "v_ma10", "v_ma20"]
df.columns = ["day", "high", "low", "open", "close", "volumn"]

cursor.close()
conn.close()

df["ma5"] = df["close"].rolling(5).mean()
df["ma10"] = df["close"].rolling(10).mean()
df["ma20"] = df["close"].rolling(20).mean()
df["v_ma5"] = df["volumn"].rolling(5).mean()
df["v_ma10"] = df["volumn"].rolling(10).mean()
df["v_ma20"] = df["volumn"].rolling(20).mean()
df["percent_change"] = df["close"].pct_change()

si = StockInsider(code=stockcode, df=df)
df_macd = si.macd()

df["macd"] = df_macd["macd"]

print(df)

# stock_data = go.Candlestick(
#             x=df["Date"],
#             open=df["Open"],
#             high=df["High"],
#             low=df["Low"],
#             close=df["Close"],
#             increasing_line_color="red",
#             decreasing_line_color="blue",
#             name="stock price",
#         )
# ma5data = go.Scatter(x=df["Date"], y=df['ma5'], name='ma5', marker_color="green")
# ma10data = go.Scatter(x=df["Date"], y=df['ma10'], name='ma10', marker_color="yellow")
# ma20data = go.Scatter(x=df["Date"], y=df['ma20'], name='ma20', marker_color="pink")
# v_ma5data = go.Scatter(x=df["Date"], y=df['v_ma5'], name='v_ma5', marker_color="green")
# v_ma10data = go.Scatter(x=df["Date"], y=df['v_ma10'], name='v_ma10', marker_color="yellow")
# v_ma20data = go.Scatter(x=df["Date"], y=df['v_ma20'], name='v_ma20', marker_color="pink")
#
# layout = go.Layout(xaxis=dict(type="category", tickangle=270))
# fig = go.Figure(data=[stock_data, ma5data, ma10data, ma20data], layout=layout)
# fig.show()
