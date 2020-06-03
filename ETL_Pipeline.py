"""
In this program, I present an end-to-end project on the Extract, Transform, and Load (ETL) process. 
Specification requirements are Pandas, NumPy, and SQLite.

Programmer: Phuong Van Nguyen
phuong.nguyen@economics.uni-kiel.de
"""


from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import numpy as np
get_ipython().run_line_magic('reload_ext', 'sql')
import sqlite3
Purple= '\033[95m'
Cyan= '\033[96m'
Darkcyan= '\033[36m'
Blue = '\033[94m'
Green = '\033[92m'
Yellow = '\033[93m'
Red = '\033[91m'
Bold = "\033[1m"
Reset = "\033[0;0m"
Underline= '\033[4m'
End = '\033[0m'

class extract():
    
    def __init__(self):
        print(Bold+ 'Extract'+ End+': data from the csv file')
        self.name_file='crypto-markets.csv'
        self.crypto_df = self.load_data(self.name_file) 
        self.no_obs=5
        self.showed_df =self.show_data(self.crypto_df,self.no_obs)
        
    def load_data(self,name_file):
        print('I am loading data\n...')
        self.data=pd.read_csv(name_file)
        print('I am done!')
        return self.data
    
    def show_data(self,data,no_obs):
        print('The first %d observations'%no_obs +'\n...')
        self.data=data.head(no_obs).T
        #print(self.data)
        print('I a done showing data!')
        return self.data
        
class transform():
    
    def __init__(self,extract):
        print(Bold+'Transform'+ End+': the extracted data')
        self.extract_data=extract.crypto_df
        print('Data manipulation and cleansing\n.....')
        self.name_col='asset'
        self.list_money=self.list_currency(self.extract_data,self.name_col)
        print('I am converting currency\n...')
        self.assetsCode = ['BTC','ETH','XRP','LTC']
        self.extract_data['open'] = self.convert_currency(self.extract_data[['open', 'asset']], self.assetsCode)
        self.extract_data['close']= self.convert_currency(self.extract_data[['close', 'asset']],self.assetsCode)
        self.extract_data['high'] = self.convert_currency(self.extract_data[['high', 'asset']], self.assetsCode)
        self.extract_data['low']  = self.convert_currency(self.extract_data[['low', 'asset']], self.assetsCode)
        self.convert_data=self.extract_data
        #display(self.convert_data.head(5).T)
        print('Converted successfully!')
        
        self.drop_data=self.drop_row(self.convert_data)
        self.reset_data=self.reset_id(self.drop_data)         
        self.col_drop=['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread']
        self.dropc_data=self.drop_column(self.reset_data,self.col_drop)
        self.prepare_data=self.convert_sql(self.dropc_data)
    
    def list_currency(self,data,name_col):
        print('The number of cryptocurrencies: %d'%len(data[name_col].unique()))
        self.data=data['asset'].unique()
        print('The list of cryptocurrencies:')
        print(self.data)
        return self.data

    def convert_currency(self,data,currency_code):
        self.data=data.apply(lambda x: (float(x[0]) * 0.75) if x[1] in currency_code else np.nan, axis=1)
        return self.data
    
    def drop_row(self,data):
        print('I am dropping rows with null values by the asset column\n...')
        self.data=data.dropna(inplace=False)
        #display(self.data.head(5).T)
        return self.data
    
    def reset_id(self,data):
        print('I am reseting the index\n...')
        self.data=data.reset_index(drop=True ,inplace=False)
        #display(self.data.head(5).T)
        print('reset the index successfully!')
        return self.data
    
    def drop_column(self,data,nam_drop):
        print('I am dropping the irrelevant columns\n...')
        self.data=data.drop(labels=nam_drop, inplace=False, axis=1)
        #display(self.data.head(5))
        print('dropped the irrelevant columns successfully!')
        return self.data
    
    def convert_sql(self,data):
        print('I am preparing data for load\n...')
        self.data=data.values.tolist()
        #display(self.data[0:4])
        print('preparation is done!')
        return self.data

class load():
    
    def __init__(self,transform):
        print(Bold+'Load'+End+': the transformed data to the SQL database')
        self.loaded_data=transform.dropc_data#prepare_data
        self.showed_data=self.list_data(self.loaded_data,3)
        self.path=r'C:\Users\Phuong_1\Documents\PhuongDatabase\Phuong_database.db'
        self.connect=self.connect_database(self.path)
        self.explore=self.explor_database()
        #self.new_table=self.creat_table()
        self.insert=self.insert_data(self.loaded_data)
        self.table_explor=self.explor_table()
         
    def list_data(self,data,ranges):
        print('The first %d arrays in the data for loading:'%ranges)
        self.data=data.head(ranges)# [ranges]
        display(self.data)
             
    def connect_database(self,path):
        print('I am trying to connect to the existed database\n....')
        self.connect = get_ipython().run_line_magic('sql', 'sqlite:///path')
        print('connection is success!')
        return self.connect
    
    def explor_database(self):
        print('I am exploring tables in this database\n...\nThe existing tables')
        self.list_table = get_ipython().run_line_magic('sql', "SELECT name FROM sqlite_master WHERE type='table'")
        print(self.list_table)
        
    def creat_table(self):
        self.del_table = get_ipython().run_line_magic('sql', 'DROP TABLE IF EXISTS Cryptocurrency')
        self.creatable = get_ipython().run_line_magic('sql', 'CREATE TABLE Cryptocurrency(ASSET TEXT NOT NULL,NAME TEXT NOT NULL,Date datetime,Open Float DEFAULT 0,High Float DEFAULT 0,Low Float DEFAULT 0,Close Float DEFAULT 0)')
        print ('The'+Bold+'Cryptocurrency'+End+' table has just been created successfully!') 
        
    def insert_data(self,Cryptocurrency):
        self.del_table = get_ipython().run_line_magic('sql', 'DROP TABLE IF EXISTS Cryptocurrency')
        #self.insert_tab=%sql INSERT INTO Cryptocurrency SELECT * FROM data
        self.insert = get_ipython().run_line_magic('sql', 'PERSIST Cryptocurrency')
        print(self.insert)
        print('I inserted data successfully!')
        return self.insert
        
    def explor_table(self):
        print('I am exloring the'+Bold+' Cryptocurrency'+End+' table')
        self.data = get_ipython().run_line_magic('sql', 'SELECT * FROM data LIMIT 5')
        display(self.data)
        return self.data       
         
if __name__=='__main__':
    load(transform(extract()))
