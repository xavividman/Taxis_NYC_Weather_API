import datetime
import numpy as np
import pandas as pd


from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ssl_args = {'ssl': {'ca': '/etc/ssl/certs/ca-certificates.crt'}}
engine = create_engine('mysql://user:passsword/database', connect_args=ssl_args)

#Create payment_type table

p_type = {'id_payment_type': [1, 2, 3, 4, 5, 6], 'type': ['Credit card', 'Cash', 'No charge', 'Dispute' ,'Unknown', 'Voided trip']}
df_p_type = pd.DataFrame(p_type)
print('payment_tye table created')


df_p_type.to_sql('payment_type', con = engine, if_exists = 'replace', index=False, method='multi')
print('payment_type tableuploaded')





#Create ratecode table

ratecode = {'id_ratecode': [1, 2, 3, 4, 5, 6], 'descript': ['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester' ,'Negotiated fare', 'Group ride']}
df_ratecode = pd.DataFrame(ratecode)
print('rate_code table created')



df_ratecode.to_sql('rate_code', con = engine, if_exists = 'replace', index=False, method='multi')
print('rate_code table uploaded')



#Create vendor table

vendor = {'id_vendor': [1, 2], 'descript': ['Creative Mobile Technologies', 'VeriFone Inc.']}
df_vendor = pd.DataFrame(vendor)
print('vendor table created')



df_vendor.to_sql('vendor', con = engine, if_exists = 'replace', index=False, method='multi')
print('vendor table uploaded')


#Create borough table

df_borough = pd.read_csv('borough.csv')
print('borough table created')

df_borough.to_sql('borough', con = engine, if_exists = 'replace', index=False, method='multi')
print('borough table uploaded')



#Create zone table

df_zone = pd.read_csv('zone.csv')
print('zone table created')


df_zone.to_sql('zone', con = engine, if_exists = 'replace', index=False, method='multi')
print('zone table uploaded')


#Create weather table

df_weather = pd.read_csv('data_weather.csv')
print('weather table created')


df_weather.to_sql('weather', con = engine, if_exists = 'replace', index=False, method='multi')
print('weather table uploaded')


#Create weather_snow table

df_weather_snow = pd.read_csv('weather_snow.csv')
print('weather_snow table created')


df_weather_snow.to_sql('weather_snow', con = engine, if_exists = 'replace', index=False, method='multi')
print('weather_snow table uploaded')


