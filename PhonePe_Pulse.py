### LOADING ALL DATA FRAME TO MYSQL
                                   
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


################################################ BUILDING STREAMLIT APPLICATION ###########################################################

conn = pymysql.connect(user = 'root',
password = '1997',
host = '127.0.0.1',
database = 'phonepe')
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
#option = st.radio('**Select your option**',('All India', 'State wise','District wise','Pincode wise'),horizontal=True)

types = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
#types_5 = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))

sidebar_width_css = """
<style>
[data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
    width: 400px;  /* Change this value to your desired width */
}
[data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
    width: 600px;  /* Change this value to your desired width when collapsed */
    margin-left: -400px;  /* Negate the width here to hide sidebar */
}
</style>
"""

# Inject the custom CSS into Streamlit
st.markdown(sidebar_width_css, unsafe_allow_html=True)


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

def convert_to_lakh(value):
            return f"{value / 1e5:.2f} Lakhs"

def convert_to_crore(value):
            return f"{value / 1e7:.2f} Crores"

def format_number(value):
                
                if value >= 1e7:
                    return convert_to_crore(value)
                elif value >= 1e5:
                    return convert_to_lakh(value)
                else:
                    return f"{value:.2f}"

selected = st.tabs(["About","Top Charts","Explore Data"])


with selected[0]:
        
        st.markdown("## :violet[About]")
        
        empty_col1, colum1, colum2, empty_col2 = st.columns([1, 2, 800, 1])
        with colum1:
            Year = st.sidebar.selectbox("**Year**", (2018,2019,2020,2021,2022,2023,2024))
            Quarter = st.sidebar.selectbox("Quarter",(1,2,3,4))
            type_trans = st.sidebar.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
                'Merchant payments','Financial Services','Others'),key='type_trans')
        
        with colum2:
            st.info(
                    """
                    ### From this Application we can get insights like :
                    - Overall Transaction Analysis.
                    - Top 10 State, District, Pincode based on Total amount spent on phonepe.
                    - Top 10 State, District, Pincode based on Total phonepe users.
                    """
                    )
            
with selected[1]:    


#FOR TRANSACTION
    if types=="Transactions":
            
        
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
            
            
            
        # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) as Averaage FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
            tr_amt_res = cursor.fetchall()
            df_tr_amt_res= pd.DataFrame(np.array(tr_amt_res), columns=['Total','Average'])
            df_tr_amt_res_1 = df_tr_amt_res.set_index(['Average'])
            print(df_tr_amt_res_1)

            
            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}';")
            tr_cnt_res = cursor.fetchall()
            df_tr_cnt_res = pd.DataFrame(np.array(tr_cnt_res), columns=['Total','Average'])
            df_tr_cnt_res_1 = df_tr_cnt_res.set_index(['Average'])
            #print(df_tr_cnt_res_1)
            
        
            
            df_tr_tab_res.drop(columns=['State'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data_1 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names = [feature['properties']['ST_NM'] for feature in data_1['features']]
            state_names.sort()
            print()
            # Create a DataFrame with the state names column
            #df_state_names_tra = pd.DataFrame({'State': state_names})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt_
            df_tr_res_1['State'] = df_tr_res_1['State'].str.title()
            df_tr_res_1['State'] = df_tr_res_1['State'].str.replace("-", " ")
            #df_state_names_tra['Transaction_amount']=df_tr_res_1['Transaction_amount']
            # convert dataframe to csv file
            df_tr_res_1.to_csv('State_trans.csv', index=False)
            #df_tr_res_1.to_csv('State_trans1.csv', index=False)
            # Read csv
            df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='blackbody',title = 'Overall Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
    elif types == "Users":
        cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{Year}' AND Quarter = '{Quarter}' GROUP BY State;")
        in_us_tab_res= cursor.fetchall()
        df_in_us_tab_rslt = pd.DataFrame(np.array(in_us_tab_res), columns=['State', 'User Count'])
        df_in_us_tab_rslt1 = df_in_us_tab_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_rslt)+1)))

        # Total User Count table query
        cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{Year}' AND Quarter = '{Quarter}';")
        in_us__res= cursor.fetchall()
        df_us_rslt = pd.DataFrame(np.array(in_us__res), columns=['Total','Average'])
        df_us_rslt1 = df_us_rslt.set_index(['Average'])
        
            
        
            
        df_in_us_tab_rslt.drop(columns=['State'], inplace=True)
            # Clone the gio data
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        
        data_2 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
        state_names = [feature['properties']['ST_NM'] for feature in data_2['features']]
        state_names.sort()
        print()
            # Create a DataFrame with the state names column
            #df_state_names_tra = pd.DataFrame({'State': state_names})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt_
        df_in_us_tab_rslt1['State'] = df_in_us_tab_rslt1['State'].str.title()
        df_in_us_tab_rslt1['State'] = df_in_us_tab_rslt1['State'].str.replace("-", " ")
            #df_state_names_tra['Transaction_amount']=df_tr_res_1['Transaction_amount']
            # convert dataframe to csv file
        df_in_us_tab_rslt1.to_csv('State_trans.csv', index=False)
            #df_tr_res_1.to_csv('State_trans1.csv', index=False)
            # Read csv
        df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
        fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='User Count',color_continuous_scale='blackbody',title = 'Overall User Analysis')
        fig_tra.update_geos(fitbounds="locations", visible=False)
        fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
        st.plotly_chart(fig_tra,use_container_width=True)
           

with selected[2]:
    top_n = st.selectbox("**Select Number of Records**", (5, 10, 15, 20))
    st.subheader(f"Top {top_n} Charts")
    types_1 = st.selectbox("**Category**", ("State", "District","Postal Code"), key="category_1")
    if types == "Transactions":#data_type = st.radio('**Select Data Type**', ('Transaction Data', 'User Data'), horizontal=True)

        

        if types_1=="State":
                st.markdown(f"### Top 20 Transactions in State")
                cursor.execute(f"""
                            SELECT State, Transaction_count, Transaction_amount
                            FROM aggregated_transaction WHERE
                            Year = '{Year}' AND Quarter = '{Quarter}' AND Transaction_type = '{type_trans}' 
                            ORDER BY Transaction_amount DESC
                             LIMIT {top_n};
                        """)
                top_transactions = cursor.fetchall()
                df_top_transactions = pd.DataFrame(top_transactions, columns=['State', 'Transaction_amount', 'Transaction_count'])
                df_top_transactions['Transaction_amount'] = df_top_transactions['Transaction_amount'].apply(format_number)
                df_top_transactions['Transaction_count'] = df_top_transactions['Transaction_count'].apply(lambda x: f"{x:,}")

                st.table(df_top_transactions)

        elif types_1=="District":
                st.markdown(f"### Top 20 Transactions in District")
                cursor.execute(f"""
                        SELECT District, Transaction_count, Transaction_amount
                        FROM map_transaction WHERE
                        Year = '{Year}' AND Quarter = '{Quarter}'
                        ORDER BY Transaction_amount DESC
                        LIMIT {top_n};
                    """)
                top_transactions_dis = cursor.fetchall()
                df_top_transactions_dis = pd.DataFrame(top_transactions_dis, columns=['District', 'Transaction_amount', 'Transaction_count'])
                df_top_transactions_dis['Transaction_amount'] = df_top_transactions_dis['Transaction_amount'].apply(format_number)
                df_top_transactions_dis['Transaction_count'] = df_top_transactions_dis['Transaction_count'].apply(lambda x: f"{x:,}")

                st.table(df_top_transactions_dis)
        else :
                st.markdown(f"### Top 20 Transactions in Postal Code")
                cursor.execute(f"""
                        SELECT District_Pincode, Transaction_count, Transaction_amount
                        FROM top_transaction WHERE
                        Year = '{Year}' AND Quarter = '{Quarter}'
                        ORDER BY Transaction_amount DESC
                        LIMIT {top_n};
                    """)
                top_transactions_post = cursor.fetchall()
                df_top_transactions_dis_po = pd.DataFrame(top_transactions_post, columns=['Postal Code', 'Transaction_amount', 'Transaction_count'])
                df_top_transactions_dis_po['Transaction_amount'] = df_top_transactions_dis_po['Transaction_amount'].apply(format_number)
                df_top_transactions_dis_po['Transaction_count'] = df_top_transactions_dis_po['Transaction_count'].apply(lambda x: f"{x:,}")
                st.table(df_top_transactions_dis_po)
    elif types == "Users":
        if types_1=="State":
                    
                st.markdown(f"### Top 20 UserCount in State")
                cursor.execute(f"""
                                    SELECT State,SUM(User_Count) as us
                                    FROM aggregated_user WHERE
                                    Year = '{Year}' AND Quarter = '{Quarter}' group by State
                                    ORDER BY us DESC
                                    LIMIT {top_n};
                                """)
                top_User = cursor.fetchall()
                #print(top_User)
                df_top_user= pd.DataFrame(top_User, columns=['State', 'User Count'])
                        
                st.table(df_top_user)

        elif types_1=="District":
                            
                            st.markdown(f"### Top 20 UserCount in District")
                            cursor.execute(f"""
                                    SELECT District, SUM(Registered_User) as us
                                    FROM map_user WHERE
                                    Year = '{Year}' AND Quarter = '{Quarter}' group by District
                                    ORDER BY us DESC
                                    LIMIT {top_n};
                                """)
                            top_us_dis = cursor.fetchall()
                            df_top_us_dis = pd.DataFrame(top_us_dis, columns=['District', 'User Count'])
                           
                            st.table(df_top_us_dis)

        else :
                            st.markdown(f"### Top 20 UserCount in Postal Code")
                            cursor.execute(f"""
                                    SELECT District_Pincode, sum(Registered_User) as us
                                    FROM top_user WHERE
                                    Year = '{Year}' AND Quarter = '{Quarter}' group by District_Pincode
                                    ORDER BY us DESC
                                    LIMIT {top_n};
                                """)
                            top_us_post = cursor.fetchall()
                            df_top_us_dis_po = pd.DataFrame(top_us_post, columns=['Postal Code', 'User Count'])
                            
                            st.table(df_top_us_dis_po)

            
                
          

        


##########################################################################################################################################

                
            
            

        