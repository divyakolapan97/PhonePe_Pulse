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


### LOADING ALL DATA FRAME TO MYSQL
                                   


username = 'root'
password = '1997'
host = '127.0.0.1'
database = 'phonepepulse'

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

################################################ BUILDING STREAMLIT APPLICATION ###########################################################
conn = pymysql.connect(user = 'root',
password = '1997',
host = '127.0.0.1',
database = 'phonepepulse')
cursor = conn.cursor()
import plotly.express as px
st.set_page_config(layout='wide')
import requests
import subprocess
# Title
st.header(':blue[Phonepe Pulse Data Visualization ]')
from streamlit_option_menu import option_menu
import numpy as np
# Selection option
option = st.radio('**Select your option**',('All India', 'State wise','District wise','Pincode wise'),horizontal=True)

types = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))

# # selected = st.sidebar.option_menu("Menu", ["Home","Top Charts","Explore Data","About"])
# # Creating option menu in the side bar
# # with st.sidebar:
# #     selected = option_menu("Menu", ["Home","Top Charts","Explore Data","Abosut"], 
                
# #                 styles={"nav-link": {"font-size": "10px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F38AD"},
# #                         "nav-link-selected": {"background-color": "#3F36AD"}})


background_color = "#F6D6D6"  # Replace with your desired background color

page_bg_color = f"""
<style>
[data-testid="stAppViewContainer"] {{
    background-color: {background_color};
}}

[data-testid="stHeader"] {{
    background: rgba(0, 0, 0, 0);
}}

[data-testid="stSidebar"] > div:first-child {{
    background-color: {background_color};
}}

[data-testid="stToolbar"] {{
    right: 2rem;
}}
</style>
"""

st.markdown(page_bg_color, unsafe_allow_html=True)

selected = st.tabs(["Top Charts","Explore Data","About"])

with selected[0]:
    st.markdown("## :violet[Top Charts]")
    #types = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    colum1,colum2= st.columns([1,1.5],gap="large")
    with colum1:
        Year = st.sidebar.selectbox("**Year**", (2018,2019,2020,2021,2022,2023,2024))
        Quarter = st.sidebar.selectbox("Quarter",(1,2,3,4))
        type_trans = st.sidebar.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'),key='type_trans')
    
    with colum2:
        st.info(
                """
                ### From this menu we can get insights like :
                - Top 10 State, District, Pincode based on Total number of transaction and Total amount spent on phonepe.
                - Top 10 State, District, Pincode based on Total phonepe users and their app opening frequency.
                - Top 10 mobile brands and its percentage based on the how many people use phonepe.
                - Overall ranking on a particular Year and Quarter.
                """,icon="🔍"
                )
    # Transaction Analysis bar chart query
        cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
        tr_res = cursor.fetchall()
        df_tr_res = pd.DataFrame(np.array(tr_res), columns=['State', 'Transaction_amount'])
        df_tr_res_1 = df_tr_res.set_index(pd.Index(range(1, len(df_tr_res)+1)))      
        print(df_tr_res_1)  

    # Transaction Analysis table query
        cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
        tr_tab_res = cursor.fetchall()
        df_tr_tab_res = pd.DataFrame(np.array(tr_tab_res), columns=['State','Transaction_count','Transaction_amount'])
        df_tr_tab_res_1 = df_tr_tab_res.set_index(pd.Index(range(1, len(df_tr_tab_res)+1)))
        print(df_tr_tab_res_1)

        # # Total Transaction Amount table query
        # cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) as Averaage FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
        # tr_amt_res = cursor.fetchall()
        # df_tr_amt_res= pd.DataFrame(np.array(tr_amt_res), columns=['Total','Average'])
        # df_tr_amt_res_1 = df_tr_amt_res.set_index(['Average'])
        
        # # Total Transaction Count table query
        # cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
        # tr_cnt_res = cursor.fetchall()
        # df_tr_cnt_res = pd.DataFrame(np.array(tr_cnt_res), columns=['Total','Average'])
        # df_tr_cnt_res_1 = df_tr_cnt_res.set_index(['Average'])

        # ### Visuals
        # df_tr_tab_res.drop(columns=['State'], inplace=True)
        # # Clone the gio data
        # url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        # response = requests.get(url)
        # data_1 = json.loads(response.content)
        # # Extract state names and sort them in alphabetical order
        # state_names = [feature['properties']['ST_NM'] for feature in data_1['features']]
        # state_names.sort()
        # print()
        # # Create a DataFrame with the state names column
        # df_state_names_tra = pd.DataFrame({'State': state_names})
        # # Combine the Gio State name with df_in_tr_tab_qry_rslt
        # df_state_names_tra['Transaction_amount']=df_tr_tab_res
        # # convert dataframe to csv file
        # df_state_names_tra.to_csv('State_trans.csv', index=False)
        # # Read csv
        # df_tra = pd.read_csv('State_trans.csv')
        # # Geo plot
        # fig_tra = px.choropleth(
        #     df_tra,
        #     geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        #     featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
        # fig_tra.update_geos(fitbounds="locations", visible=False)
        # fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
        # st.plotly_chart(fig_tra,use_container_width=True)
        




