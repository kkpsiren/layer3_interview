import streamlit as st

import requests
import json
import time
from queries import *
import os
import pandas as pd 

class Flipsider:
    def __init__(self, API_KEY, TTL_MINUTES=60*24):
        self.API_KEY = API_KEY
        self.TTL_MINUTES = TTL_MINUTES
        
        self.endpoint_url = 'https://node-api.flipsidecrypto.com/queries' 
        self.headers = {"Accept": "application/json", "Content-Type": "application/json", "x-api-key": self.API_KEY}


    def print_expection(self,r):
        return f"Error creating query, got response: {r.text} with status code: {str(r.status_code)}"
    def create_query(self, SQL_QUERY):
        r = requests.post(
            self.endpoint_url, 
            data=json.dumps({
                "sql": SQL_QUERY,
                "ttlMinutes": self.TTL_MINUTES
            }),
            headers=self.headers
        )
        if r.status_code != 200:
            raise Exception(self.print_expection(r))

        return json.loads(r.text)    


    def get_query_results(self, token):
        r = requests.get(
            f'{self.endpoint_url}/{token}',
            headers=self.headers
        )
        if r.status_code != 200:
            raise Exception(self.print_expection(r))
        
        data = json.loads(r.text)
        if data['status'] == 'running':
            time.sleep(10)
            return self.get_query_results(token)

        return data

    def run(self, SQL_QUERY):
        query = self.create_query(SQL_QUERY)
        token = query.get('token')
        data = self.get_query_results(token)
        df = pd.DataFrame(data['results'],columns = data['columnLabels'])
        return df


st.cache()
def load_queries():
    df = pd.read_csv('query1.csv',index_col=0)
    df2 = pd.read_csv('query2.csv',index_col=0)
    df3 = pd.read_csv('query3.csv',index_col=0)
    df4 = pd.read_csv('query4.csv',index_col=0)
    return df,df2,df3,df4


def add_across(df):
    addresses = list(set(['0x69B5c72837769eF1e7C164Abc6515DcFf217F920'.lower(),
    '0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C'.lower(),
    '0x69B5c72837769eF1e7C164Abc6515DcFf217F920'.lower(),
    '0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C'.lower(),
    '0x69B5c72837769eF1e7C164Abc6515DcFf217F920'.lower(),
    '0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C'.lower(),
    '0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9'.lower(),
    '0x4D9079Bb4165aeb4084c526a32695dCfd2F77381'.lower(),
    '0x69B5c72837769eF1e7C164Abc6515DcFf217F920'.lower(),
    '0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C'.lower(),
    '0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9'.lower(),
    '0x4D9079Bb4165aeb4084c526a32695dCfd2F77381'.lower()]))
    df['CONTRACT_ADDRESS'].isin(addresses)
    return df
    
st.cache()
def run_queries():
    bot = Flipsider(os.getenv('API_KEY'))
    df = bot.run(QUERY)
    df2 = bot.run(QUERY2)
    df3 = bot.run(QUERY3)
    df4 = bot.run(QUERY4)
    df4 = add_across(df4).dropna(subset='PROJECT_NAME',axis=1)
    
    return df,df2,df3,df4