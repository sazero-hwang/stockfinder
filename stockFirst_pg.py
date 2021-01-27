import FinanceDataReader as fdr
from sqlalchemy import create_engine

# engine = create_engine('postgresql://sazero:sazero@localhost:5432/stockdb')
engine = create_engine('postgresql://testuser:testpwd@192.168.10.199:5432/testdb')

# sql = '''DROP TABLE public.stockcomp;
# sql = '''    CREATE TABLE public.stockcomp (
#         "index" int8 NULL,
#         "Symbol" varchar(20) NULL,
#         "Market" text NULL,
#         "Name" text NULL,
#         "Sector" text NULL,
#         "Industry" text NULL,
#         "ListingDate" timestamp NULL,
#         "SettleMonth" text NULL,
#         "Representative" text NULL,
#         "HomePage" text NULL,
#         "Region" text NULL
#     );
#     CREATE INDEX ix_stockcomp_index ON public.stockcomp USING btree (index);
#     CREATE INDEX stockcomp_symbol_idx ON public.stockcomp USING btree ("Symbol");'''
# engine.execute(sql)
#
# # sql = '''DROP TABLE public.stockdata;
# sql = '''    CREATE TABLE public.stockdata (
#         "Date" timestamp NULL,
#         "Open" int8 NULL,
#         "High" int8 NULL,
#         "Low" int8 NULL,
#         "Close" int8 NULL,
#         "Volume" int8 NULL,
#         "Change" float8 NULL,
#         "Symbol" varchar(20) NULL,
#         "ma5" float8 DEFAULT NULL,
#         "ma10" float8 DEFAULT NULL,
#         "ma20" float8 DEFAULT NULL,
#         "v_ma5" float8 DEFAULT NULL,
#         "v_ma10" float8 DEFAULT NULL,
#         "v_ma20" float8 DEFAULT NULL
#     );
#     CREATE INDEX "ix_stockdata_Date" ON public.stockdata USING btree ("Date");
#     CREATE INDEX "stockdata_symbol_idx" ON public.stockdata USING btree ("Symbol");'''
# engine.execute(sql)

def compinfo():
    import psycopg2
    import pandas as pd
    constr = "host='localhost' dbname='stockdb' user='sazero' password='sazero'"
    con = psycopg2.connect(constr)
    cur = con.cursor()
    sql = 'select  "Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region" from "stockcomp"'
    cur.execute(sql)
    result = cur.fetchall()
    return pd.DataFrame(result)



df = compinfo()
df.columns = ["Symbol", "Market", "Name", "Sector", "Industry", "ListingDate", "SettleMonth", "Representative", "HomePage", "Region"]
df.to_sql('stockcomp', engine, if_exists='append')

for index, row in df.iterrows():
    print(index, row['Symbol'], row['Name'])
    df_comp = fdr.DataReader(row['Symbol'])
    if len(df_comp) == 0:
        continue
    df_comp['Symbol'] = row['Symbol']
    df_comp["ma5"] = df_comp["Close"].rolling(5).mean()
    df_comp["ma10"] = df_comp["Close"].rolling(10).mean()
    df_comp["ma20"] = df_comp["Close"].rolling(20).mean()
    df_comp["v_ma5"] = df_comp["Volume"].rolling(5).mean()
    df_comp["v_ma10"] = df_comp["Volume"].rolling(10).mean()
    df_comp["v_ma20"] = df_comp["Volume"].rolling(20).mean()
    df_comp.to_sql('stockdata', engine, if_exists='append')
    print(f'{len(df_comp)} inserted')
