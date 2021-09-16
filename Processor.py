#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 17:35:01 2021

@author: chunkeichan
"""

# Required libraries.
# !pip install backtrader
# Remark: Current version of matplotlib is 3.3.4 which is not compatible with backtrader library. Thus, installing 3.2.2 matplotlib is necessary.
# Remark: Please do in Terminal.
# pip uninstall matplotlib
# !pip install matplotlib==3.2.2
# !pip install python-dateutil
# !pip install SQLAlchemy


import yfinance as yf
import datetime
import backtrader as bt
import pandas as pd
from dateutil.relativedelta import relativedelta
import backtrader.feeds as btfeeds
import requests
from bs4 import BeautifulSoup
from Data_feeder import Data_5mins_retrieval, Data_1day_retrieval, Data_bktest_retrieval, Data_SPXconstit_retrieval, FiveMins_data, save_to_mysql
import time
import mysql.connector


class IntergratedStartegy(bt.Strategy):
    # For BackTesting, spx_current=0, vix_current=1, spx_future=0, vix_future=1.
    # For Trading, spx_current=8, vix_current=9, spx_future=10, vix_future=11.
    params = (('size_SPX', 1),
              ('size_VIX', 100),
              ('spx_current', 0),
              ('vix_current',1),
              ('spx_future', 0),
              ('vix_future', 1),)

    # [Optional] Use if necessary.    
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))
    
    # [Optional] Use if necessary.
    def notify_data(self, data, status):
        print('Data Status =>', data._getstatusname(status))
        if status == data.LIVE:
            self.data_ready = True

    # [Optional] Use if necessary.
    def notify_store(self, msg, *args, **kwargs):
        if 'error' in str(msg):
            print(msg)

    def __init__(self):
        self.time_init = time.perf_counter()
        
        self.counter = 1
        # print("__init__ started")
        self.tic = time.perf_counter()
        self.toc = time.perf_counter()
        
        # Track each effective trade.
        self.trade_log = pd.DataFrame(columns=["Reg_Code","Datetime","Stock","Action","Shares","Price"])
        self.ind_datas = pd.DataFrame(columns=["Datetime","ich_senkou_span_a","ich_senkou_span_b","spx_5mins_50SMA","spx_5mins_20SMA","spx_5mins_150SMA","spx_5mins_stochastic","spx_5mins_MACD","spx_5mins_200SMA","spx_5mins_MACDEMA","spx_5mins_12RSI"])
        self.perf_datas = pd.DataFrame(columns=["Datetime","value","SNP500"])        
        self.mk_signal = pd.DataFrame(columns=["Datetime","AoF_VIX","AoF_VVIX","AoF_EconCal","AoF_signal","TP_SPT","TP_PaMA","TP_ConDi","TP_signal","stra_ichi","stra_MovMom","stra_rsi2","stra_macd"])
        
        # Buy order
        self.VIXL_code001 = 1
        self.VIXS_code001 = 1        
        self.SPXL_code001 = 1
        self.SPXL_code002 = 1
        self.SPXL_code003 = 1
        self.SPXL_code004 = 1
        
        # Settlement
        # Unique
        self.SPXL_code003_settle = 1
        # General
        self.VIXL_code001_settle = 1
        self.VIXS_code001_settle = 1        
        self.SPXL_code001_settle = 1
        self.SPXL_code002_settle = 1        
        self.SPXL_code004_settle = 1
        
        self.ich = bt.indicators.Ichimoku(self.datas[self.p.spx_current])
        
        self.spx_5mins_50SMA = bt.indicators.SimpleMovingAverage(self.datas[0].close, period=50)
        
        self.vix_10day_EMA = bt.indicators.ExponentialMovingAverage(self.datas[5].close, period=10)
        self.vvix_10day_EMA = bt.indicators.ExponentialMovingAverage(self.datas[6].close, period=10, plot=False)
        
        self.long_vix = -1

        self.AoF_VIX = 1
        self.AoF_VVIX = 1
        self.AoF_EconCal = 1     
        self.AoF_signal = 1

        self.TP_SPT = 0
        self.TP_PaMA = 0
        self.TP_ConDi = 0        
        self.TP_signal = 0

        self.stra_ichi = 0                    
        self.stra_MovMom = 0
        self.stra_rsi2 = 0
        self.stra_macd = 0

        # Moving Momentum 
        self.spx_5mins_20SMA = bt.indicators.SimpleMovingAverage(self.datas[0].close, period=20)
        self.spx_5mins_150SMA = bt.indicators.SimpleMovingAverage(self.datas[0].close, period=150)
        self.spx_5mins_stochastic = bt.indicators.Stochastic(self.datas[0])
        
        self.spx_5mins_MACD = bt.indicators.MACD(self.datas[0].close)

        # RSI2
        self.spx_5mins_200SMA = bt.indicators.SimpleMovingAverage(self.datas[0].close, period=200)
        
        # MACD     
        self.spx_5mins_MACDEMA = bt.indicators.ExponentialMovingAverage(self.spx_5mins_MACD)
        self.spx_5mins_12RSI = bt.indicators.RelativeStrengthIndex(self.datas[0].close, period=12)


    def MarketAnalyzer_AoF(self):
        # VIX
        vix_5mins_low = (self.datas[self.p.vix_current].low[0] + self.datas[0].low[0] + self.datas[0].low[-1] + self.datas[0].low[-2] + self.datas[0].low[-3]) / 5
        vix_10day_EMA_now = self.vix_10day_EMA[0]
        # VVIX
        vvix_5mins_low = (self.datas[2].low[0] + self.datas[2].low[-1] + self.datas[2].low[-2] + self.datas[2].low[-3] + self.datas[2].low[-4]) / 5
        vvix_10day_EMA_now = self.vvix_10day_EMA  
        
        currentDateTime = self.datetime.datetime(ago=0)
        curDateStr = currentDateTime.strftime('%Y-%m-%d')   
        
        if vix_5mins_low > vix_10day_EMA_now:
            # Long VIX future
            self.AoF_VIX = 1

        elif vix_5mins_low <= vix_10day_EMA_now:
            self.AoF_VIX = 0

            
        ######CONFIDENTIAL######
        #.
        #.
        #.
        ######CONFIDENTIAL######       


        if self.AoF_VVIX == 0 :
            self.AoF_signal = 0
    
    
    def MarketAnalyzer_TP(self):                
        # Slope Performance Trend (period = 1-day)
        SPT_slope = (self.datas[4].open[0] - self.datas[4].close[-13]) / 14
        # Remark: 10 is the fluctuation of SPX by observation. 
        SPT_ind = (10/2)/14 
        
        if SPT_slope > SPT_ind:
            self.TP_SPT = 1
        elif SPT_slope < SPT_ind:
            self.TP_SPT = -1
        else:
            self.TP_SPT = 0

        ######CONFIDENTIAL######
        #.
        #.
        #.
        ######CONFIDENTIAL######   
        
        # Short Summary
        if self.TP_SPT  > 1:
            self.TP_signal = 1
        elif self.TP_SPT   < -1:
            self.TP_signal = -1
        else:
            self.TP_signal = 0
            
            
    def next(self):
        if self.toc - self.tic > 5 * 60:
            FiveMins_data()
        self.tic = time.perf_counter()
        
        def settlement_VIX():
            if self.VIXL_code001_settle < self.VIXL_code001:
                self.trade_log.loc[len(self.trade_log)]=[f"VIXL1{str(self.VIXL_code001_settle).zfill(6)}",self.datas[self.p.vix_current].datetime.datetime(0),"VIX","Close",self.p.size_VIX,self.datas[self.p.vix_future][0]]
                self.VIXL_code001_settle += 1    
            if self.VIXS_code001_settle < self.VIXS_code001:
                self.trade_log.loc[len(self.trade_log)]=[f"VIXS1{str(self.VIXS_code001_settle).zfill(6)}",self.datas[self.p.vix_current].datetime.datetime(0),"VIX","Close",self.p.size_VIX,self.datas[self.p.vix_future][0]]
                self.VIXS_code001_settle += 1    
        def settlement_SPX():
            if self.SPXL_code001_settle < self.SPXL_code001:
                self.trade_log.loc[len(self.trade_log)]=[f"SPXL1{str(self.SPXL_code001_settle).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Close",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                self.SPXL_code001_settle += 1    
            if self.SPXL_code002_settle < self.SPXL_code002:
                self.trade_log.loc[len(self.trade_log)]=[f"SPXL2{str(self.SPXL_code002_settle).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Close",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                self.SPXL_code002_settle += 1    
            if self.SPXL_code003_settle < self.SPXL_code003:
                self.trade_log.loc[len(self.trade_log)]=[f"SPXL3{str(self.SPXL_code003_settle).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Close",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                self.SPXL_code003_settle += 1    
            if self.SPXL_code004_settle < self.SPXL_code004:
                self.trade_log.loc[len(self.trade_log)]=[f"SPXL4{str(self.SPXL_code004_settle).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Close",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                self.SPXL_code004_settle += 1  
        
        def my_market_close():
            if self.datas[self.p.spx_current].datetime.time(0) >= datetime.time(10, 30):
                if (not self.getposition(data=self.datas[self.p.spx_future]).size == 0):
                    self.close(data=self.datas[self.p.spx_current])
                    settlement_SPX()
                if (not self.getposition(data=self.datas[self.p.vix_future]).size == 0):
                    self.close(data=self.datas[self.p.vix_current])  
                    settlement_VIX()
                return 1
            else:
                return 0

        market_time = my_market_close()
        # Counting the opeartion time from initiation to market close.
        if market_time == 1:
            self.counter += 1
            return
        else:
            # Activate Market Analyzer(Avoidance of Fluctuation)
            self.MarketAnalyzer_AoF()
            if not self.AoF_signal == 0:
                self.close(data=self.datas[self.p.spx_current])
                settlement_SPX()
            else:
                # Activate Market Analyzer(Trend Prediction)
                self.MarketAnalyzer_TP()  
                if self.TP_signal == 1:
                    
                    # Activate bullish strategy


        ######CONFIDENTIAL######
        #.
        #.
        #.
        ######CONFIDENTIAL######   
   
                 
                    # RSI2
                    rsi2_signal = 0
                    spx_5mins_close = (self.datas[self.p.spx_current].close[0] + self.datas[0].close[-1] + self.datas[0].close[-2] + self.datas[0].close[-3] + self.datas[0].close[-4]) / 5
                    if spx_5mins_close > self.spx_5mins_200SMA[0]:
                        rsi2_signal += 1
                    min_rsi_200d = 99999999
                    for i in range(-(200-12),1,-1):
                        if min_rsi_200d > self.spx_5mins_12RSI[i]:
                            min_rsi_200d = self.spx_5mins_12RSI[i]
                    if self.spx_5mins_12RSI[0] <= 15 + min_rsi_200d * 0.85:
                        rsi2_signal += 1
            
                    if rsi2_signal == 2:
                        self.stra_rsi2 = 1                        
                        if self.getposition(data=self.datas[self.p.spx_future]).size == 0:
                            self.buy(data=self.datas[self.p.spx_future], size=self.p.size_SPX)
                            self.trade_log.loc[len(self.trade_log)]=[f"SPXL3{str(self.SPXL_code003).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Long",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                            self.SPXL_code003 += 1              
                    else:
                        self.stra_rsi2 = 0
                    # To settle the above order.
                    if self.spx_5mins_12RSI >= 15 + min_rsi_200d * 0.85 + 50:
                        if not self.getposition(data=self.datas[self.p.spx_future]).size == 0:
                            self.sell(data=self.datas[self.p.spx_future], size=self.p.size_SPX)
                            self.trade_log.loc[len(self.trade_log)]=[f"SPXL3{str(self.SPXL_code003_settle).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Close",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                            self.SPXL_code003_settle += 1
                            
                    # MACD
                    if self.spx_5mins_MACDEMA[0] < self.spx_5mins_MACD[0]:
                        self.stra_macd = 1                        
                        if self.getposition(data=self.datas[self.p.spx_future]).size == 0:
                            self.buy(data=self.datas[self.p.spx_future], size=self.p.size_SPX)      
                            self.trade_log.loc[len(self.trade_log)]=[f"SPXL4{str(self.SPXL_code004).zfill(6)}",self.datas[self.p.spx_current].datetime.datetime(0),"SPX","Long",self.p.size_SPX,self.datas[self.p.spx_future][0]]
                            self.SPXL_code004 += 1   
                    else:
                        self.stra_macd = 0                               
    
               # For SPX futures
                previous_peak = 0
                # Calculate MDD in 1 day.
                for i in range(-83,1,-1):
                    if self.datas[0].close[i] < self.datas[0].close[i-1] and self.datas[0].close[i-1] > self.datas[0].close[i-2] and self.datas[0].close[i] * 1.01 < self.datas[0].close[i-2] and previous_peak < self.datas[0].close[i-1]:
                        previous_peak = self.datas[0].close[i-1] 
                MDD = (previous_peak - self.datas[self.p.spx_current].close[0]) / self.datas[self.p.spx_current].close[0]
                
                if MDD >= 0.05:
                    self.close(data=self.datas[self.p.spx_current])
                    settlement_SPX()

                def take_profit(stock="VIX", value=self.p.spx_future, settle=settlement_SPX()):
                    total_cost = 0
                    total_shares = 0
                    trade_exist_raw1 = self.trade_log.drop_duplicates(subset=['Reg_Code'],keep=False)
                    trade_exist = trade_exist_raw1.set_index('Stock')
                    if stock in trade_exist.index:
                           trade_exist = trade_exist.drop([stock])
                    if not len(trade_exist["Reg_Code"]) == 0:
                        print(trade_exist)               
                    
                    if not (self.getposition(data=self.datas[value]).size == 0):
                        for share in trade_exist["Shares"]:
                            total_shares += share
                            for price in trade_exist["Price"]:
                                # print(f'share: {share}, price: {price}')
                                cost = share * price
                                total_cost += cost
                        if not total_cost == 0:
                            profit = (total_shares * self.datas[value].close[0] - total_cost) / total_cost
        
                            previous_peak = 0
                            for i in range(-56,1,-1):
                                if self.datas[0].close[i] < self.datas[0].close[i-1] and self.datas[0].close[i-1] > self.datas[0].close[i-2] and self.datas[0].close[i] * 1.01 < self.datas[0].close[i-2]:
                                    previous_peak = self.datas[0].close[i-1] 
                            drawdown = (previous_peak - self.datas[self.p.spx_current].close[0]) / self.datas[value].close[0]
                
                            if profit > 0.01 and drawdown > 0.1:
                                self.close(data=self.datas[value])
                                settle
                
                # Check if there is a need for taking profit for both SPX and VIX future.
                take_profit()
                take_profit(stock="SPX", value=self.p.vix_future, settle=settlement_VIX())
                
                # For VIX futures
                previous_peak = 0
                for i in range(-56,1,-1):
                    if self.datas[1].close[i] < self.datas[1].close[i-1] and self.datas[1].close[i-1] > self.datas[1].close[i-2] and self.datas[1].close[i] * 1.01 < self.datas[1].close[i-2]:
                        previous_peak = self.datas[1].close[i-1] 
                        # print(previous_peak)
                drawdown = (previous_peak - self.datas[self.p.vix_current].close[0]) / self.datas[self.p.vix_current].close[0]
                
                if drawdown >= 0.05:
                    self.close(data=self.datas[self.p.vix_current])
                    settlement_VIX()


        ######CONFIDENTIAL######
        #.
        #.
        #.
        ######CONFIDENTIAL######   
              
        
        # End of strategies.
        self.toc = time.perf_counter()            
        self.counter += 1
        
        # Record signal data.
        ind_data = pd.Series([self.datas[0].datetime.datetime(0), self.ich.l.senkou_span_a[0], self.ich.l.senkou_span_b[0], self.spx_5mins_50SMA[0],self.spx_5mins_20SMA[0],self.spx_5mins_150SMA[0],self.spx_5mins_stochastic[0],self.spx_5mins_MACD[0],self.spx_5mins_200SMA[0],self.spx_5mins_MACDEMA[0],self.spx_5mins_12RSI[0]], index=["Datetime","ich_senkou_span_a","ich_senkou_span_b","spx_5mins_50SMA","spx_5mins_20SMA","spx_5mins_150SMA","spx_5mins_stochastic","spx_5mins_MACD","spx_5mins_200SMA","spx_5mins_MACDEMA","spx_5mins_12RSI"])
        self.ind_datas = self.ind_datas.append(ind_data, ignore_index=True)
        
        perf_data = pd.Series([self.datas[0].datetime.datetime(0), self.broker.getvalue(), self.datas[0].close[0]], index=["Datetime","value","SNP500"])
        self.perf_datas = self.perf_datas.append(perf_data, ignore_index=True)
        
        mk_sig = pd.Series([self.datas[0].datetime.datetime(0), self.AoF_VIX, self.AoF_VVIX, self.AoF_EconCal, self.AoF_signal, self.TP_SPT, self.TP_PaMA, self.TP_ConDi, self.TP_signal, self.stra_ichi, self.stra_MovMom, self.stra_rsi2, self.stra_macd], index=["Datetime","AoF_VIX","AoF_VVIX","AoF_EconCal","AoF_signal","TP_SPT","TP_PaMA","TP_ConDi","TP_signal","stra_ichi","stra_MovMom","stra_rsi2","stra_macd"])
        self.mk_signal = self.mk_signal.append(mk_sig, ignore_index=True)
        
        global ind_datas
        ind_datas = self.ind_datas
        global trade_log
        trade_log = self.trade_log
        global perf_datas
        perf_datas = self.perf_datas
        global mk_signal
        mk_signal = self.mk_signal

def start_backtesting():
    # Update bktest data.
    # Information to be filled by you.    
    mydb = mysql.connector.connect(
        host="XXXXXXX",
        user="XXXXXXX",
        password="XXXXXXX")
    mycursor = mydb.cursor()
    mycursor.callproc("ib_bktest.accum_data")
    mycursor.close()
    mydb.close()

    data_spx_5mins = Data_bktest_retrieval("SPX", "5mins", plot=True)
    data_vix_5mins = Data_bktest_retrieval("VIX", "5mins", plot=True)
    data_vvix_5mins = Data_bktest_retrieval("VVIX", "5mins")
    data_naq_5mins = Data_bktest_retrieval("NAQ", "5mins")
    data_spx_1day = Data_bktest_retrieval("SPX", "1day")
    data_vix_1day = Data_bktest_retrieval("VIX", "1day")
    data_vvix_1day = Data_bktest_retrieval("VVIX", "1day")
    data_naq_1day = Data_bktest_retrieval("NAQ", "1day")

    cerebro = bt.Cerebro()

    cerebro.adddata(data_spx_5mins)
    cerebro.adddata(data_vix_5mins)
    cerebro.adddata(data_vvix_5mins)
    cerebro.adddata(data_naq_5mins)
    cerebro.adddata(data_spx_1day)
    cerebro.adddata(data_vix_1day)
    cerebro.adddata(data_vvix_1day)
    cerebro.adddata(data_naq_1day)

    cerebro.addstrategy(IntergratedStartegy)
    
    cerebro.broker.set_cash(cash=10000)

    cerebro.broker.set_shortcash(False)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer,_name="Basic_stats")
    cerebro.addobserver(bt.observers.Value)

    strats = cerebro.run()
    strat = strats[0]
    print('Final Balance: %.2f' % cerebro.broker.getvalue())
    for e in strat.analyzers:
        e.print()
    cerebro.plot(volume=False,width=100, height=100)
    
    # Save signal data to mySQL.
    print(ind_datas)
    print(trade_log) 
    print(perf_datas)
    print(mk_signal)
    
    for item in [[ind_datas,"ind_datas"],
                 [trade_log, "trade_log"],
                 [perf_datas, "perf_datas"],
                 [mk_signal, "mk_signal"]]:
        save_to_mysql(df=item[0], file_name=item[1], db="ib_trade")        
    
    # Remark: Since Spyder itself cannot print a HTML and Javascript graph, QT5 and the following codes are needed.
    import backtrader.plot
    import matplotlib
    matplotlib.use('QT5Agg')
    cerebro.plot(iplot= False)


def start_trading():
    to_mysql=False
    
    data_spx_5mins = Data_5mins_retrieval("SPX", "^GSPC", plot=True, to_mysql=to_mysql)
    data_vix_5mins = Data_5mins_retrieval("VIX", "^VIX", plot=True, to_mysql=to_mysql)
    data_vvix_5mins = Data_5mins_retrieval("VVIX", "^VVIX", to_mysql=to_mysql)
    data_naq_5mins = Data_5mins_retrieval("NAQ", "^IXIC", to_mysql=to_mysql)
    data_spx_1day = Data_1day_retrieval("SPX", "^GSPC", to_mysql=to_mysql)
    data_vix_1day = Data_1day_retrieval("VIX", "^VIX", to_mysql=to_mysql)
    data_vvix_1day = Data_1day_retrieval("VVIX", "^VVIX", to_mysql=to_mysql)
    data_naq_1day = Data_1day_retrieval("NAQ", "^IXIC", to_mysql=to_mysql)

    cerebro = bt.Cerebro()
    cerebro.adddata(data_spx_5mins)
    cerebro.adddata(data_vix_5mins)
    cerebro.adddata(data_vvix_5mins)
    cerebro.adddata(data_naq_5mins)
    cerebro.adddata(data_spx_1day)
    cerebro.adddata(data_vix_1day)
    cerebro.adddata(data_vvix_1day)
    cerebro.adddata(data_naq_1day)
    
    # Information to be filled by you.
    store = bt.stores.IBStore(port=XXXXXXX, clientId=XXXXXXX)
    
    data_spx_current = store.getdata(dataname='ES', sectype='FUT', exchange='GLOBEX', timeframe=bt.TimeFrame.Minutes)
    data_vix_current = store.getdata(dataname='VX', sectype='STK', exchange='GLOBEX', timeframe=bt.TimeFrame.Minutes)
    data_spx_future = store.getdata(dataname='ES', sectype='FUT', exchange='GLOBEX', timeframe=bt.TimeFrame.Minutes)
    data_vix_future = store.getdata(dataname='VX', sectype='STK', exchange='GLOBEX', timeframe=bt.TimeFrame.Minutes)
    cerebro.resampledata(data_spx_current, timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.resampledata(data_vix_current, timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.resampledata(data_spx_future, timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.resampledata(data_vix_future, timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.adddata(data_spx_current)
    cerebro.adddata(data_vix_current)
    cerebro.adddata(data_spx_future)
    cerebro.adddata(data_vix_future)

    cerebro.broker = store.getbroker()
    cerebro.addstrategy(IntergratedStartegy)      
    cerebro.run()


if __name__ == "__main__":
    # Please choose backtesting or live trading.
    start_backtesting()
    # start_trading()