# Import the required libraries
import pandas as pd
import numpy as np
import os 
import glob 
import json
import csv
import pyarrow as pa
from pyarrow import Table 
import pyarrow.parquet as pq
import pyarrow.csv as pv
import pandasql as ps


#Folder path of source file
path=r'dataset'
os.listdir(path)
# List all files in a directory using os.listdir
json_pattern = os.path.join(path, '*.json')
file_list = glob.glob(json_pattern)

for fs in file_list:  
    print(fs)
    df= pd.read_json(fs)
    #print(df)
    
#define the schema 

def get_list_of_json_files():
    path=r'dataset'
    os.listdir(path)
    # List all files in a directory using os.listdir
    json_pattern = os.path.join(path, '*.json')
    list_of_files = glob.glob(json_pattern)    
    return list_of_files

def create_list_from_json(jsonfile):
    with open(jsonfile) as f:
        data = json.load(f)
        #print(data)
        df2= pd.json_normalize(data, "attributes")
        #df1=pd.DataFrame(data)
        df2.to_csv('output.csv',mode='a', encoding='utf-8',index=False)
        
       
def write_csv(path):
    list_of_files = get_list_of_json_files()
    #print(list_of_files)
    
    for file in list_of_files:
        #path1=path+'\\'+file
        create_list_from_json(file)  # create the row to be added to csv for each file (json-file)

# Save the csv file in parquet format and partion on the basis of Status.
def convert_csvToParquet(csvfile):
        df=pd.read_csv(csvfile)
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table,root_path='output1.parquet',partition_cols=['STATUS'])
        pandas_df=pd.read_parquet('output1.parquet')
        print(pandas_df)

# Get the total sales value of the cancelled orders
def query_total_sales_cancelled(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    sqlquery = "select sum(SALES) from pandas_df where STATUS='Cancelled'"
    print(ps.sqldf(sqlquery))

#  total sales value of the orders currently on hold for the year 2005
def query_totalsales_2005(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    sqlquery = "select sum(SALES) from pandas_df where STATUS='On Hold' and ORDERDATE like '%2005%'"
    print(ps.sqldf(sqlquery))

# Count of distinct products per product line
def query_count_distinct_productlines(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    sqlquery = "select PRODUCTLINE,count(*) from pandas_df group by PRODUCTLINE"
    print(ps.sqldf(sqlquery))

# Total sales variance for sales calculated at both sales price and MSRP
def query_variance(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    sqlquery_sales = "select sum(SALES)from pandas_df"
    sqlquery_expected = "select sum(MSRP*QUANTITYORDERED)from pandas_df"
    value_sales=ps.sqldf(sqlquery_sales)
    value_expected=ps.sqldf(sqlquery_expected)
    #total_variance=sqlquery_expected-sqlquery_sales
    print("Variance"+value_sales)

# Percentage change in sales YoY for classic cars
def query_PerChangeClassicCars(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    sqlquery_2004 = "select sum(SALES) from pandas_df where STATUS='Shipped' and ORDERDATE like '%2004%'"
    sqlquery_2005 = "select sum(SALES) from pandas_df where STATUS='Shipped' and ORDERDATE like '%2005%'"
    value_sales_2004=ps.sqldf(sqlquery_2004)
    value_expected_2005=ps.sqldf(sqlquery_2005)
    print(value_expected_2005)
    per_Change=((value_expected_2005-value_expected_2004)/value_expected_2004)*100
    total_variance=sqlquery_expected-sqlquery_sales
    print("PerChangeClassicCars"+total_variance) 

# Percentage change in sales YoY for classic cars
def query_FilterProductLine(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    print(pandas_df[(pandas_df['PRODUCTLINE'] == 'Vintage Cars') | (pandas_df['PRODUCTLINE'] == 'Classic Cars') | (pandas_df['PRODUCTLINE'] == 'Motorcycles') | (pandas_df['PRODUCTLINE'] == 'Trucks and Buses') ])
    # print(pandas_df)

# Qunatity Based discounts
def query_QuantityBasedDiscount(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    pandas_df['QUANTITYORDERED'] = pd.to_numeric(pandas_df.QUANTITYORDERED, errors='coerce').fillna(0).astype(np.int64)
    pandas_df['SALES'] = pd.to_numeric(pandas_df.SALES, errors='coerce').fillna(0).astype(np.int64)
    pandas_df['DiscountedSale']=pandas_df['SALES']
    for index,item in pandas_df.iterrows():       
        if item["QUANTITYORDERED"]<=30:
            pandas_df.iloc[index,7]= pandas_df.iloc[index,2]
        if item["QUANTITYORDERED"]>30 & item["QUANTITYORDERED"] <= 60:
            pandas_df.iloc[index]['DiscountedSale']= pandas_df.iloc[index]['SALES']*0.975
        if item["QUANTITYORDERED"] >60 & item["QUANTITYORDERED"] <=80:
            pandas_df.iloc[index]['DiscountedSale']= pandas_df.iloc[index]['SALES']*0.96
        if item["QUANTITYORDERED"] >80 & item["QUANTITYORDERED"] <=100:
            pandas_df.iloc[index]['DiscountedSale']= pandas_df.iloc[index]['SALES']*0.94
        if item["QUANTITYORDERED"] >100:
            pandas_df.iloc[index]['DiscountedSale']= pandas_df.iloc[index]['SALES']*0.90
    print(pandas_df)

def query_recalculateMSRP(parquetfile):
    pandas_df=pd.read_parquet('output1.parquet')
    pandas_df['QUANTITYORDERED'] = pd.to_numeric(pandas_df.QUANTITYORDERED, errors='coerce').fillna(0).astype(np.int64)
    pandas_df['SALES'] = pd.to_numeric(pandas_df.SALES, errors='coerce').fillna(0).astype(np.int64)
    pandas_df['RecalculatedSales']=pandas_df['SALES']/pandas_df['QUANTITYORDERED'] 
    print(pandas_df)

if __name__=="__main__": 
    path=r'dataset'
    # Total sales value of the cancelled orders
    query_total_sales_cancelled('output1.parquet')
    
    # Total sales value of the orders currently on hold for the year 2005
    query_totalsales_2005('output1.parquet')

    # Cunt of distinct products per product line
    query_count_distinct_productlines('output1.parquet')

    # total sales variance for sales calculated at both sales price and MSRP
    query_variance('output1.parquet')

    #  percentage change in sales YoY for classic cars, for years 2004 and 2005 where the status is shipped
    query_PerChangeClassicCars('output1.parquet')

    # Filter the data from productline
    query_FilterProductLine('output1.parquet')

    # Quantity based sales column
    query_QuantityBasedDiscount('output1.parquet')

    # Calculated column by recalculating sales using MSRP.
    query_recalculateMSRP('output1.parquet')
    