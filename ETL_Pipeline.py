#!/usr/bin/env python
# coding: utf-8

# ![ETL.png](attachment:ETL.png)

# # Extracting data
# Basically, You can extract data from any data sources such as Files, any RDBMS/NoSql Database, Websites or real-time user activity. In this project, we extract data from CSV file below.
# 
# > crypto-markets.csv

# In[6]:


import pandas as pd
crypto_df = pd.read_csv('crypto-markets.csv')
display(crypto_df.head())
print('The number of cryptocurrencies: %d'%len(crypto_df.asset.unique()))
print(crypto_df.asset.unique())


# **Warning**: 
# 
# These prices are in USD and we want to save this price into GBP currency (Great Britain Pound). Also, let’s assume that some columns are irrelevant for us, so we will drop those columns in the end. Since our data does not contain any Null or blank values and its kind of structure as well so we can skip data cleansing part.
# 
# # Transforming the extracted data
# 
# Transformation is in itself a two steps process below.
# 
# - Data manipulation.
# - Data cleansing.
# 
# 

# ## Data manipulation
# In this project, let’s start with transforming the data. Transformation logic is to convert the price of BTC, ETH , XRP and LTC cryptocurrency only into GBP from USD. Let’s just assume that we don’t care about other currencies.
# 
# We convert open, close, high and low prices of crypto currencies into GBP values since current price is in Dollars
#  if currency belong to this list of BTC, ETH , XRP and LTC cryptocurrencies.
# 
# ### List of BTC, ETH , XRP and LTC cryptocurrencies

# In[7]:


import numpy as np
assetsCode = ['BTC','ETH','XRP','LTC']


# ### Converting into GBP
# > 1 USD = 0.80 GBP

# In[9]:


crypto_df['open'] = crypto_df[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['close'] = crypto_df[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['high'] = crypto_df[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['low'] = crypto_df[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)


# In[11]:


# dropping rows with null values by asset column
crypto_df.dropna(inplace=True)

# reset the data frame index
crypto_df.reset_index(drop=True ,inplace=True)


# ### Showing the result

# In[12]:


display(crypto_df.head())


# **Warnimng**:
# 
# There are a lot of columns, let's assume that for us, relevant columns are only asset, name, date, open, high, low and close. So let’s drop other irrelevant columns, such as below.
# 
# > ['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread']
# 
# ## Data cleansing

# In[14]:


crypto_df.drop(labels=['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread'], inplace=True, axis=1)
display(crypto_df.head())


# # Loading the transformed Data
# It is worth noting that One thing to keep in mind is that loading part could also be in the form of Data Visualizations (Graphs), PDF or Excel report, or database as in our case. However, in this project, in this final step, we load the transformed Data in SQL database.

# ## Creating a SQL connection
# In this project, we will create and connect to a SQlite database. However, one can make connection with other databases like Oracle, DB2 etc. such as # import cx_Oracle 'username/password@hostname:port/service_name'

# ### Creating a SQL database

# In[15]:


import sqlite3
# connect function opens a connection to the SQLite database file, 
conn = sqlite3.connect('Phuong_session.db')
print(conn)


# ### Drop a table name Crypto if it exists already
# This step is optional

# In[16]:


try:
    conn.execute('DROP TABLE IF EXISTS `Crypto` ')
except Exception as e:
    raise(e)
finally:
    print('Table dropped')


# ### Creating a new Table named as Crypto

# In[17]:


try:
    conn.execute('''
         CREATE TABLE Crypto
         (ID         INTEGER PRIMARY KEY,
         ASSET       TEXT    NOT NULL,
         NAME        TEXT    NOT NULL,
         Date        datetime,
         Open        Float DEFAULT 0,
         High        Float DEFAULT 0,
         Low         Float DEFAULT 0,
         Close       Float DEFAULT 0);''')
    print ("Table created successfully");
except Exception as e:
    print(str(e))
    print('Table Creation Failed!!!!!')
finally:
    conn.close() # this closes the database connection


# ## Loading data to the created SQL database
# For loading data to the created SQLite database, we again need to change our data from Pandas Dataframe to Python List of Lists or List of Tuples. This is because that's the format sqlite module understand for data insertion. 
# ### Changing Pandas Dataframe

# In[18]:


crypto_list = crypto_df.values.tolist()
print(crypto_list)


# ### Making new connection to Insert crypto data in SQL DB

# In[19]:


conn = sqlite3.connect('Phuong_session.db')


# ### Making a cursor
# It will help with querying SQL DB

# In[20]:


cur = conn.cursor()


# ### Loading our transformed data to the SQL database

# In[22]:


try:
    cur.executemany("INSERT INTO Crypto(ASSET, NAME, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?,?)", crypto_list)
    conn.commit()
    print('Data Inserted Successfully')
except Exception as e:
    print(str(e))
    print('Data Insertion Failed')
finally:
    # finally block will help with always closing the connection to DB even in case of error.
    conn.close()


# ### Veerifing it

# In[23]:


conn = sqlite3.connect('Phuong_session.db')
rows = conn.cursor().execute('Select * from Crypto')
# print(rows[:2])
for row in rows:
    print(row)
conn.close()


# In[ ]:




