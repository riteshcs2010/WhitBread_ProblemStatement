from datetime import datetime

import datetime

df=datetime.datetime.strptime("11/12/2003 0:00", "%d/%m/%Y %H:%M").strftime("%Y-%m-%d")
df
print(df)
# inDate = "29-Apr-2013-15:59:02"
# d = datetime.strptime(inDate, "%d-%b-%Y-%H:%M:%S")
# d
# datetime.datetime(2013, 4, 29, 15, 59, 2)
# d.strftimeme("YYYYMMDD HH:mm:ss (%Y%m%d %H:%M:%S)")

import pandas as pd


df=pd.read_csv('output.csv',parse_dates=['ORDERDATE'])
print(type(df.ORDERDATE))