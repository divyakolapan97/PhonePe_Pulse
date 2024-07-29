import os
import pandas as pd
import json
import sqlalchemy
import mysql.connector as connection
import mysql.connector
from sqlalchemy import create_engine
import mysql.connector
import streamlit as st
import pymysql


path = r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\aggregated\transaction\country\india\state"
state_list = os.listdir(path)
#print(state_list)

#TRANSFORMING AGGREGATED TRANSACTION LIST

group_transc = {'State':[],"Year":[],'Quarter':[],"Transaction_type":[],'Transaction_count':[],'Transaction_amount':[]}
for i in state_list:
    path_i = path+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            for z in dd['data']['transactionData']:
                type_n = z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                group_transc['Transaction_type'].append(type_n)
                group_transc['Transaction_count'].append(count)
                group_transc['Transaction_amount'].append(amount)
                group_transc['State'].append(i)
                group_transc['Year'].append(j)
                group_transc['Quarter'].append(int(k.strip('.json')))   
aggre_trans = pd.DataFrame(group_transc)
#print(aggre_trans)    
aggre_trans['State'] = aggre_trans['State'].astype(str)          


#TRANSFORMING AGGREGATED USER LIST
path_users = r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\aggregated\user\country\india\state"
user_list = os.listdir(path)
#print(user_list)
group_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}
for i in user_list:
    path_i = path_users+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            try:
                for z in dd['data']['usersByDevice']:
                    brands = z['brand']
                    count=z['count']
                    Percent=z['percentage']
                    group_user['Brands'].append(brands)
                    group_user['User_Count'].append(count)
                    group_user['User_Percentage'].append(Percent)
                    group_user['State'].append(i)
                    group_user['Year'].append(j)
                    group_user['Quarter'].append(int(k.strip('.json'))) 

            except:
                pass  

        
aggre_user = pd.DataFrame(group_user)
#print(aggre_user)

#TRANSFORMING MAP TRANSACTION LIST

path_map_trans = r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\map\transaction\hover\country\india\state"
map_trans_list = os.listdir(path_map_trans)
#print(user_list)
group_map_trans = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}

for i in map_trans_list:
    path_i = path_map_trans+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            try:
                for z in dd['data']["hoverDataList"]:
                    District = z["name"]
                    count = z["metric"][0]["count"]
                    amount = z["metric"][0]["amount"]
                    group_map_trans['District'].append(District)
                    group_map_trans['Transaction_Count'].append(count)
                    group_map_trans['Transaction_Amount'].append(amount)
                    group_map_trans['State'].append(i)
                    group_map_trans['Year'].append(j)
                    group_map_trans['Quarter'].append(int(k.strip('.json'))) 

            except:
                pass  

        
map_trans= pd.DataFrame(group_map_trans)
#print(map_trans)


#TRANSFORMING MAP USERS LIST

path_map_user= r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\map\user\hover\country\india\state"
map_user_list = os.listdir(path_map_user)
print(map_user_list)
group_map_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], "Registered_User": []}

for i in map_user_list:
    path_i = path_map_user+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            #print(dd)
            try:
                for z in dd['data']["hoverData"].items():
                    District = z[0]
                    #print(District)
                    Registered_User = z[1]["registeredUsers"]
                    #print(Registered_User)
                    group_map_user['District'].append(District)
                    group_map_user['Registered_User'].append(Registered_User)
                    group_map_user['State'].append(i)
                    group_map_user['Year'].append(j)
                    group_map_user['Quarter'].append(int(k.strip('.json'))) 

            except:
                pass  

#print(dd)
map_users= pd.DataFrame(group_map_user)
#print(map_users)

#TRANSFORMING TOP TRANSCATION LIST



path_top_trans= r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\top\transaction\country\india\state"
top_trans_list = os.listdir(path_top_trans)
#print(top_trans_list)
group_top_trans = {'State': [], 'Year': [], 'Quarter': [],  'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in top_trans_list:
    path_i = path_top_trans+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            #print(dd)
            try:
                for z in dd['data']["pincodes"]:
                    Name = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    group_top_trans['District_Pincode'].append(Name)
                    group_top_trans['Transaction_count'].append(count)
                    group_top_trans['Transaction_amount'].append(amount)

                   
                    group_top_trans['State'].append(i)
                    group_top_trans['Year'].append(j)
                    group_top_trans['Quarter'].append(int(k.strip('.json'))) 

            except:
                pass  

#print(dd)
top_trans= pd.DataFrame(group_top_trans)
print(top_trans)

#TRANSFORMING TOP USER LIST



path_top_user= r"C:\Users\DELL\Documents\Guvi\Project_2_PhonePe\pulse\data\top\user\country\india\state"
top_user_list = os.listdir(path_top_user)
#print(top_trans_list)
group_top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}
for i in top_user_list:
    path_i = path_top_user+"/"+i
    year1 = os.listdir(path_i)
    for j in year1:
        path_j = path_i+"/"+j
        quat=os.listdir(path_j)
        for k in quat:
            path_k=path_j+"/"+k
            data = open(path_k,'r')
            dd = json.load(data)
            #print(dd)
            try:
                for z in dd['data']["pincodes"]:
                    Name = z['name']
                    count = z['registeredUsers']
                    
                    group_top_user['District_Pincode'].append(Name)
                    group_top_user['Registered_User'].append(count)
                    group_top_user['State'].append(i)
                    group_top_user['Year'].append(j)
                    group_top_user['Quarter'].append(int(k.strip('.json'))) 

            except:
                pass  

#print(dd)
top_user= pd.DataFrame(group_top_user)
#print(top_user.dtypes)


username = 'root'
password = '1997'
host = '127.0.0.1'
database = 'phonepe'

# Create the SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}/{database}')

# Store the DataFrame in the MySQL table


#1)

aggre_trans.to_sql(name='aggregated_transaction', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#2)

aggre_user.to_sql(name='aggregated_user', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#3)

map_trans.to_sql(name='map_transaction', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#4)

map_users.to_sql(name='map_user', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#5)

top_trans.to_sql(name='top_transaction', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#4)

top_user.to_sql(name='top_user', con=engine, if_exists='replace', index=False)

print("DataFrame successfully stored into MySQL database")

#connection = mydb.connect()
#connection.close()
