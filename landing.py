import streamlit as st
import pandas as pd
from scripts import load_queries
from plots import * 
from queries import *
from utils import *

import seaborn as sns
pd.set_option('display.width', 1400)
cm = sns.light_palette("green", as_cmap=True)


def landing_page():
    #st.sidebar.image("https://static.wixstatic.com/media/7c96bd_25e85c48fc9a4b3f992a5f82f6b7a4cc~mv2.png/v1/fit/w_2500,h_1330,al_c/7c96bd_25e85c48fc9a4b3f992a5f82f6b7a4cc~mv2.png",width=300)
    st.sidebar.title("Layer3 x Across Protocol")
    st.sidebar.markdown("""

                     """)


    df,df2,df3,df4 = load_queries()


    addresses = filter_df(df,col='BLOCK_TIMESTAMP', filter_dt='2022-07-23')
    addresses2 = filter_df(df2,col='BLOCK_TIMESTAMP', filter_dt='2022-07-23')

    st.header('Use your own Referral link to bridge 50 USDC from Arbitrum to Boba')
    st.markdown(f"""Transactions where the bridger is using their own Referral link.  
In total we found {addresses['ORIGIN_FROM_ADDRESS'].nunique()} Addresses.  
These addresses had done {addresses['ORIGIN_FROM_ADDRESS'].count()} transactions between {addresses['BLOCK_TIMESTAMP'].min()} and {addresses['BLOCK_TIMESTAMP'].max()}.""")


    st.markdown(' #### Polygon - Arbitrum ')
    m,l,r = st.columns([2,6,6])
    with m:
        st.markdown('#')
        st.markdown('#')
        st.markdown('#')
        st.markdown('#')
        window_length = st.number_input('Time Window (Minutes)',min_value=1,max_value=60*24,step=1,value=60)

    with l:
        st.markdown(' #### Transaction count')
        fig = plot_timeline(get_timeline(df2, window_length, x1='TX counts'), x1='TX counts')
        st.plotly_chart(fig,use_container_width=True)
    with r:
        st.markdown(' #### USDC amount Bridged')
        fig = plot_timeline(get_timeline(df2, window_length,col='AMOUNT',method='sum',x1='USDC Amount'),x1='USDC Amount')
        st.plotly_chart(fig,use_container_width=True)
        
    st.markdown(' #### Arbitrum - Boba')
    m,l,r = st.columns([2,6,6])
    with l:
        st.markdown(' #### Transaction count')
        fig = plot_timeline(get_timeline(df, window_length, x1='TX counts'), x1='TX counts')
        st.plotly_chart(fig,use_container_width=True)
    with r:
        st.markdown(' #### USDC amount Bridged')
        fig = plot_timeline(get_timeline(df, window_length,col='AMOUNT',method='sum',x1='USDC Amount'),x1='USDC Amount')
        st.plotly_chart(fig,use_container_width=True)
    
    with st.expander("Show data Polygon - Arbitrum"):
            st.dataframe(df2[['BLOCK_TIMESTAMP','TX_HASH','ORIGIN_FROM_ADDRESS','AMOUNT']])
            
    with st.expander("Show data Arbitrum - Boba"):
            st.dataframe(df[['BLOCK_TIMESTAMP','TX_HASH','ORIGIN_FROM_ADDRESS','AMOUNT']])
    with st.expander('Browse Addresses'):
        options = addresses2.groupby('ORIGIN_FROM_ADDRESS')['AMOUNT'].sum().sort_values(ascending=False).index.tolist()
        selected_addresses = st.multiselect('Select Addresses', options,default=options[2])
        df_queried = addresses.query('ORIGIN_FROM_ADDRESS in @selected_addresses')
        df_queried2 = addresses2.query('ORIGIN_FROM_ADDRESS in @selected_addresses')
        st.markdown(' #### Polygon - Arbitrum ')
        l,r,m = st.columns([3,3,4])
        with m:
            st.markdown("#")
            st.markdown("#")
            st.markdown("#")
            st.subheader(f'Selected Addresses')
            st.markdown('- '+ '\n - '.join(selected_addresses))
        with l:
            st.write('Transaction count')
            fig = plot_timeline(get_timeline(df_queried2, window_length, x1='TX counts'), x1='TX counts')
            st.plotly_chart(fig,use_container_width=True)
        with r:
            st.write('USDC amount Bridged')
            fig = plot_timeline(get_timeline(df_queried2, window_length,col='AMOUNT',method='sum',x1='USDC Amount'),x1='USDC Amount')
            st.plotly_chart(fig,use_container_width=True)
        st.markdown(' #### Arbitrum - Boba')
        l,r,m = st.columns([3,3,4])
        with l:
            st.write('Transaction count')
            fig = plot_timeline(get_timeline(df_queried, window_length, x1='TX counts'), x1='TX counts')
            st.plotly_chart(fig,use_container_width=True)
        with r:
            st.write('USDC amount Bridged')
            fig = plot_timeline(get_timeline(df_queried, window_length,col='AMOUNT',method='sum',x1='USDC Amount'),x1='USDC Amount')
            st.plotly_chart(fig,use_container_width=True)  

    st.subheader('Referral Code usage')
    l,r = st.columns(2)
    
    with l:
        st.write('Top Codes Used by all')
        _df3 = df3.groupby('REF_CODE')['ORIGIN_FROM_ADDRESS'].count().sort_values(ascending=False).to_frame('count').reset_index().head(10)
        colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
        fig = px.pie(_df3, values='count', names='REF_CODE', hole=.3)
        fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=15,
                    marker=dict(colors=colors, line=dict(color='#000000', width=0.1)))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig.update_layout(legend=dict(
    orientation="v",
    yanchor="bottom",
    y=-1.18,
    xanchor="right",
    x=1
))
        st.plotly_chart(fig,use_container_width=True)  

    with r:
        st.write('Codes used other than the issuer address')
        _df3 = df3[df3['ORIGIN_FROM_ADDRESS'].str[2:].str.lower()!=df3['REF_CODE'].str.lower()].groupby('REF_CODE')['ORIGIN_FROM_ADDRESS'].count().sort_values(ascending=False).to_frame('count').reset_index()
        colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
        fig = px.pie(_df3, values='count', names='REF_CODE', hole=.3)
        fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=15,
                    marker=dict(colors=colors, line=dict(color='#000000', width=0.1)))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig.update_layout(legend=dict(
    orientation="v",
    yanchor="bottom",
    y=-1.18,
    xanchor="right",
    x=1
))
        st.plotly_chart(fig,use_container_width=True)  
    
    st.subheader('Activity since 2022-07-24')
    st.write('Blockchains -- Number of addresses that have had activity in the corresponding blockchains')
    c = 'BLOCKCHAIN'
    _df4 = df4.groupby(c)['WALLET'].nunique().sort_values(ascending=False).to_frame('count').reset_index()
    colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
    fig = px.pie(_df4, values='count', names=c, hole=.3)
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=13,
                marker=dict(colors=colors, line=dict(color='#000000', width=0.1)))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    fig.update_layout(legend=dict(
orientation="h",
yanchor="bottom",
y=-0.28,
xanchor="left",
x=0.1
))
    st.plotly_chart(fig,use_container_width=True)  
    
    st.subheader('Treemap for all transactions')
    fig = px.treemap(df4.groupby(['BLOCKCHAIN','ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'PROJECT_NAME'])['EVENT_COUNT'].sum().to_frame('count').reset_index(), 
                     path=[px.Constant('all'),'BLOCKCHAIN','LABEL_TYPE', 'LABEL_SUBTYPE', 'PROJECT_NAME'], values='count',
                  color='BLOCKCHAIN')
    fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig,use_container_width=True)  

    st.subheader('Treemap for all activity weighted by the user and project')
    fig = px.treemap(df4.groupby(['BLOCKCHAIN','ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'PROJECT_NAME'])['WALLET'].nunique().to_frame('count').reset_index(), 
                     path=[px.Constant('all'),'PROJECT_NAME','BLOCKCHAIN','LABEL_TYPE', 'LABEL_SUBTYPE'], values='count',
                  color='BLOCKCHAIN')
    fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig,use_container_width=True)  
    
    st.subheader('Icicle Chart for all activity weighted by the user and blockchain')
    fig = px.icicle(df4.groupby(['BLOCKCHAIN','ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'PROJECT_NAME'])['WALLET'].nunique().to_frame('count').reset_index(), 
                     path=[px.Constant('all'),'BLOCKCHAIN','LABEL_TYPE', 'LABEL_SUBTYPE','PROJECT_NAME'], values='count',
                  color='BLOCKCHAIN')
    fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig,use_container_width=True)  
    
    
    
    for c in [
       'ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'PROJECT_NAME']:
        st.write(f'Activity with {c}')
        # c = 'BLOCKCHAIN'
        _df4 = df4.groupby(c)['WALLET'].nunique().sort_values(ascending=False).to_frame('count').reset_index().head(20)
        colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
        fig = px.pie(_df4, values='count', names=c, hole=.3)
        fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=13,
                    marker=dict(colors=colors, line=dict(color='#000000', width=0.1)))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-1.28,
    xanchor="left",
    x=0.1
    ))
        st.plotly_chart(fig,use_container_width=True)  
    
    
    st.sidebar.write("""#### Powered by FlipsideCrypto Godmode and ShroomDK 🫡""")

    st.subheader('SQL Queries')
    with st.expander("Query for 'Use your own Referral link to bridge 50 USDC from Arbitrum to Boba'"):
        st.markdown(f"""#### Query 1 for Use your own Referral link to bridge 50 USDC from Arbitrum to Boba
```
{QUERY}
```""")
    with st.expander("Query for 'Bridge at least 50 USDC from Polygon to Arbitrum on Across'"):
        st.markdown(f"""#### Query 2 for Bridge at least 50 USDC from Polygon to Arbitrum on Across
```
{QUERY2}
```""")
    with st.expander("Query for 'referral link activity Across'"):
        st.markdown(f"""#### Query 3 for referral link activity Across
```
{QUERY3}
```""")
    with st.expander("Query for 'user token flows'"):
        st.markdown(f"""#### Query 4 for user token flows
```
{QUERY4}
```""")