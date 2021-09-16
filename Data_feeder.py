#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 11:12:39 2021

@author: chunkeichan
"""

import yfinance as yf
import datetime
import backtrader as bt
import pandas as pd
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import mysql.connector


# Data retrieval        
def Data_5mins_retrieval(stock:str, symbol:str, plot=False, to_mysql=False):
    temp_data_SPX = yf.download(symbol, start=(datetime.datetime.now()+relativedelta(months=-2,hours=+49)), end=(datetime.datetime.now()), interval='5m', auto_adjust=True)
    temp_data_SPX = temp_data_SPX.drop_duplicates() 
    temp_data_SPX = temp_data_SPX.reset_index() 
    for row in range(len(temp_data_SPX["Datetime"])):
        dt = str(temp_data_SPX.loc[row, "Datetime"])[0:19].replace("-", "")
        temp_data_SPX.loc[row, "Datetime"] = datetime.datetime.strptime(dt, "%Y%m%d %H:%M:%S")
    temp_data_SPX = temp_data_SPX.sort_values(by=["Datetime"])
    
    def sql(stock:str, symbol:str):
        # Information to be filled by you.
        engine_trade = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="XXXXXXXX",
                                   pw="XXXXXXXX",
                                   db="XXXXXXXX"))
        temp_data_SPX.to_sql(f"Data_{stock}_5mins", con=engine_trade, if_exists='replace', chunksize=1000, index=False)

    if to_mysql:
        sql(stock, symbol)
        
    temp_data_SPX = temp_data_SPX.set_index(temp_data_SPX["Datetime"]) 
    temp_data_SPX = temp_data_SPX.drop(columns="Datetime")
    print(temp_data_SPX)
    data = bt.feeds.PandasData(dataname=temp_data_SPX, plot=plot)
    return data


def Data_1day_retrieval(stock:str, symbol:str, plot=False, to_mysql=False):
    temp_data_SPX = yf.download(symbol, start=(datetime.datetime.now()+relativedelta(months=-2,hours=+25)), end=(datetime.datetime.now()), interval='1d', auto_adjust=True)
    temp_data_SPX = temp_data_SPX.drop_duplicates() 
    temp_data_SPX = temp_data_SPX.reset_index() 
    for row in range(len(temp_data_SPX["Date"])):
        dt = str(temp_data_SPX.loc[row, "Date"]).replace("-", "")
        temp_data_SPX.loc[row, "Date"] = datetime.datetime.strptime(dt, "%Y%m%d %H:%M:%S")
    temp_data_SPX = temp_data_SPX.sort_values(by=["Date"])

    def sql(stock:str, symbol:str):
        # Information to be filled by you.
        engine_trade = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="XXXXXXXX",
                                   pw="XXXXXXXX",
                                   db="XXXXXXXX"))
        temp_data_SPX.to_sql(f"Data_{stock}_1day", con=engine_trade, if_exists='replace', chunksize=1000, index=False)

    if to_mysql:
        sql(stock, symbol)

    temp_data_SPX = temp_data_SPX.set_index(temp_data_SPX["Date"]) 
    temp_data_SPX = temp_data_SPX.drop(columns="Date")
    print(temp_data_SPX)
    data = bt.feeds.PandasData(dataname=temp_data_SPX, plot=plot)
    return data


def Data_bktest_retrieval(stock:str, period:str, plot=False):
    # Information to be filled by you.
    mydb = mysql.connector.connect(
        host="XXXXXXXX",
        user="XXXXXXXX",
        password="XXXXXXXX")
    mycursor = mydb.cursor()
    sql = f"SELECT * FROM ib_bktest.Data_{stock}_5mins"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    df = pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    for i, result in enumerate(myresult):
        df.loc[i] = result
    df = df.set_index(df["Datetime"]) 
    df = df.drop(columns="Datetime")
    print(df)
    mycursor.close()
    mydb.close()
    data = bt.feeds.PandasData(dataname=df, plot=plot)
    return data


def Data_SPXconstit_retrieval():
    # Get the latest list of S&P500 from a website.
    url = "https://www.slickcharts.com/sp500"
    # Remark: It seems the page rejects GET requests that do not identify a User-Agent. I visited the page with a browser (Chrome) and copied the User-Agent header of the GET request (look in the Network tab of the developer tools)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    SPX_weblist = requests.get(url, headers=headers)
    SPX_weblist.status_code
    SPX_list_html = SPX_weblist.text
    
    SPX_list_bs4 = BeautifulSoup(SPX_list_html)
    SPX_symbollist = []
    for row in SPX_list_bs4.find_all("tr"):
        for cell in row.find_all("td"):
            if (cell != []):
                if 'symbol' in str(cell) and len(cell.string) < 6:
                    symbol = cell.text
                    SPX_symbollist.append(symbol.replace(".","-"))
    SPX_symbollist = sorted(SPX_symbollist)

    # Calculate the Average Percent above Moving Average for S&P500 constituents.
    result = 0
    for symbol in SPX_symbollist:
        temp_data_SPX = yf.download(symbol, start=(datetime.datetime.now()+relativedelta(days=-50)), end=(datetime.datetime.now()), interval='5m', auto_adjust=True)
        temp_data_SPX = temp_data_SPX.drop_duplicates() 
        temp_data_SPX = temp_data_SPX.reset_index() 
        temp_data_SPX = temp_data_SPX.dropna()
        req_data = temp_data_SPX["Close"]
        value_50SMA = sum(req_data) / len(req_data)
        if req_data[len(req_data)-1] > value_50SMA:
            result += 1
        print(f"Stock: {symbol}, sum: {sum(req_data):.2f}, num: {len(req_data)}, {value_50SMA:.2f}, result: {result:.2f}.")
    answer = result / len(SPX_symbollist)
    print("Average Percent above Moving Average for S&P500 constituents: {:.2%}".format(answer))
    return answer


def FiveMins_data():
    data_spx_5mins = Data_5mins_retrieval("SPX", "^GSPC", plot=True)
    data_vix_5mins = Data_5mins_retrieval("VIX", "^VIX", plot=True)
    data_vvix_5mins = Data_5mins_retrieval("VVIX", "^VVIX")
    data_naq_5mins = Data_5mins_retrieval("NAQ", "^IXIC")
    

def save_to_mysql(df, file_name, db):
    # Information to be filled by you.
    engine_trade = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="XXXXXXXX",
                                pw="XXXXXXXX",
                                db=db))
    df.to_sql(file_name, con=engine_trade, if_exists='replace', chunksize=1000, index=False)