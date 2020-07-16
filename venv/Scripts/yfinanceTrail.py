import os
import sys
import time
from datetime import date
import yfinance as yp
import csv
import pandas as pd
from get_all_tickers import get_tickers as gt
from yahooquery import Ticker

#########################################################
################CREATING SPY FILE########################
today_SPY_file_name_temp = str(date.today()).replace('-','') + "_SPY_temp" + ".csv"
today_SPY_file_name = str(date.today()).replace('-','') + "_SPY" + ".csv"

spy = yp.download("SPY", start = "2019-1-1", end = str(date.today()))["Adj Close"].to_csv()

f_spy_temp = open(today_SPY_file_name_temp,"w+")
f_spy = open(today_SPY_file_name,"w+")

f_spy_temp.write(spy)

f_spy_temp.close()

with open(today_SPY_file_name_temp, newline='') as stockReading, open(today_SPY_file_name,'w', newline='') as stockEdit:
    writer = csv.writer(stockEdit)
    for row in csv.reader(stockReading):
        if len(row)>1:
            writer.writerow(row)
os.remove(today_SPY_file_name_temp)

df_spy = pd.read_csv(today_SPY_file_name)
df_spy['%SPYchange'] = None
for i in range(len(df_spy)):
    if i>=1:
        df_spy.at[i,'%SPYchange']=(df_spy.at[i,'Adj Close']-df_spy.at[i-1,'Adj Close'])/df_spy.at[i-1,'Adj Close']


#########################################################
##############CREATING ALL TICKERS LIST#################

list_of_tickers = gt.get_tickers()
with open("All_stock_list.csv","w+") as filehandle:
    filehandle.write("Stock Symbol"+"\n")
    for tickers in list_of_tickers:
        filehandle.write(tickers + "\n")

df_tickers = pd.read_csv("All_stock_list.csv")

df_tickers["Price"] = None
df_tickers["Beta"] = None
df_tickers["Net"] = None
df_tickers["Weighted Net"] = None
df_tickers.to_csv("All_stock_list.csv")

########################################################
##############CREATING GENERAL STOCK FILE################
df_ticker_list = pd.read_csv("All_Stock_List.csv")
df_ticker_list['Sector'] = None
df_ticker_list['Industry'] = None

for x in range(len(df_ticker_list)):
#for x in range(100):
    ##################CREATING PRICE COLUMN######################
    try:
        p = Ticker(df_ticker_list.at[x, 'Stock Symbol'])
        p_data = p.financial_data
        df_ticker_list.at[x, 'Price'] = p_data[str(df_ticker_list.at[x, 'Stock Symbol'])]['currentPrice']
    except:
        df_ticker_list.at[x, 'Price'] = 0
    #############################################################
    ##################CREATING BETA COLUMN######################
    try:
        bt = Ticker(df_ticker_list.at[x, 'Stock Symbol'])
        bt_data = bt.summary_detail
        df_ticker_list.at[x, 'Beta'] = bt_data[str(df_ticker_list.at[x, 'Stock Symbol'])]['beta']
    except:
        df_ticker_list.at[x, 'Beta'] = 0
    #############################################################
    ##################CREATING SECTOR & INDUSTRY#################
    try:
        t = Ticker(df_ticker_list.at[x, 'Stock Symbol'])
        t_data = t.asset_profile
        df_ticker_list.at[x, 'Sector'] = t_data[str(df_ticker_list.at[x, 'Stock Symbol'])]['sector']
        df_ticker_list.at[x, 'Industry'] = t_data[str(df_ticker_list.at[x, 'Stock Symbol'])]['industry']
    except:
        df_ticker_list.at[x, 'Sector'] = "Other"
        df_ticker_list.at[x, 'Industry'] = "Other"
    ##################################################
    ##################CALCULATING NET & WEIGHTED NET#############
    try:
        today_file_name_temp = str(date.today()).replace('-', '') + "_"+df_ticker_list.at[x,"Stock Symbol"]+"_temp" + ".csv"
        today_file_name = str(date.today()).replace('-','') + "_" + df_ticker_list.at[x,"Stock Symbol"] + ".csv"

        myData = yp.download(df_ticker_list.at[x,"Stock Symbol"], start = "2019-1-1", end = str(date.today()))["Adj Close"].to_csv()

        f_temp = open(today_file_name_temp,"w+")
        f = open(today_file_name,"w+")

        f_temp.write(myData)

        f_temp.close()

        with open(today_file_name_temp, newline='') as stockReading, open(today_file_name,'w', newline='') as stockEdit:
            writer = csv.writer(stockEdit)
            for row in csv.reader(stockReading):
                if len(row)>1:
                    writer.writerow(row)
        os.remove(today_file_name_temp)

        df = pd.read_csv(today_file_name)

        ###################################
        #######filter out day <200#########BUT NO IDEA WHY IT WORKS
        if len(df)!=len(df_spy):
            temp_stock_symbol = df.at[x,"Stock Symbol"].astype(str)
            print(temp_stock_symbol)

        #####################################
        df['%change'] = None
        for i in range(len(df)):
            if i>=1:
                df.at[i,'%change']=(df.at[i,'Adj Close']-df.at[i-1,'Adj Close'])/df.at[i-1,'Adj Close']

        ######################################
        df['%SPYchange'] = df_spy['%SPYchange']

        ######################################
        df['Case'] = None
        for i in range(len(df)):
            if i>=1:
                if df.at[i,'%SPYchange']>0 and df.at[i,'%change']>df.at[i,'%SPYchange']:
                    df.at[i,'Case'] = 1
                if df.at[i,'%SPYchange']>0 and df.at[i,'%change']<=df.at[i,'%SPYchange']:
                    df.at[i, 'Case'] = 2
                if df.at[i,'%SPYchange']>0 and df.at[i, '%change'] <= 0:
                    df.at[i, 'Case'] = 3
                if df.at[i,'%SPYchange']<=0 and df.at[i, '%change'] > 0:
                    df.at[i, 'Case'] = 4
                if df.at[i,'%SPYchange']<=0 and df.at[i,'%change']<= 0 and df.at[i,'%change']>df.at[i,'%SPYchange']:
                    df.at[i, 'Case'] = 5
                if df.at[i, '%SPYchange'] <= 0 and df.at[i,'%change']<= 0 and df.at[i,'%change']<=df.at[i, '%SPYchange']:
                    df.at[i, 'Case'] = 6

        df['Delta'] = None
        for i in range(len(df)):
            if i>=1:
                df.at[i, 'Delta'] = abs(df.at[i,'%SPYchange'] - df.at[i, '%change'])

        df['Case Total'] = None
        caseNo1 = 0
        caseNo2 = 0
        caseNo3 = 0
        caseNo4 = 0
        caseNo5 = 0
        caseNo6 = 0

        for i in range(len(df)):
            if df.at[i, 'Case'] == 1:
                caseNo1 = caseNo1 + 1
            if df.at[i, 'Case'] == 2:
                caseNo2 = caseNo2 + 1
            if df.at[i, 'Case'] == 3:
                caseNo3 = caseNo3 + 1
            if df.at[i, 'Case'] == 4:
                caseNo4 = caseNo4 + 1
            if df.at[i, 'Case'] == 5:
                caseNo5 = caseNo5 + 1
            if df.at[i, 'Case'] == 6:
                caseNo6 = caseNo6 + 1

        df.at[0,'Case Total'] = caseNo1
        df.at[1,'Case Total'] = caseNo2
        df.at[2,'Case Total'] = caseNo3
        df.at[3,'Case Total'] = caseNo4
        df.at[4,'Case Total'] = caseNo5
        df.at[5,'Case Total'] = caseNo6

        df['Case +'] = None
        df.at[0,'Case +'] = caseNo1 + caseNo4 + caseNo5

        df['Case -'] = None
        df.at[0,'Case -'] = caseNo2 + caseNo3 + caseNo6

        df['Case +%'] = None
        df.at[0,'Case +%'] = (caseNo1 + caseNo4 + caseNo5)/(caseNo1 + caseNo2 + caseNo3+ caseNo4 + caseNo5+ caseNo6)

        df['Case -%'] = None
        df.at[0,'Case -%'] = (caseNo2 + caseNo3 + caseNo6)/(caseNo1 + caseNo2 + caseNo3+ caseNo4 + caseNo5+ caseNo6)

        df['Net'] = None
        df.at[0,'Net'] = (df.at[0,'Case +%'] - df.at[0,'Case -%'])

        df["Weighted Net"] = None
        weighted_net_sum = 0
        for i in range(len(df)):
            if df.at[i, 'Case'] == 1:
                weighted_net_sum = weighted_net_sum + df.at[i, 'Delta']
            if df.at[i, 'Case'] == 2:
                weighted_net_sum = weighted_net_sum - df.at[i, 'Delta']
            if df.at[i, 'Case'] == 3:
                weighted_net_sum = weighted_net_sum - df.at[i, 'Delta']
            if df.at[i, 'Case'] == 4:
                weighted_net_sum = weighted_net_sum + df.at[i, 'Delta']
            if df.at[i, 'Case'] == 5:
                weighted_net_sum = weighted_net_sum + df.at[i, 'Delta']
            if df.at[i, 'Case'] == 6:
                weighted_net_sum = weighted_net_sum - df.at[i, 'Delta']
        df.at[0, 'Weighted Net'] = weighted_net_sum

        #########################################
        df_ticker_list.at[x,"Net"] = df.at[0,'Net']
        df_ticker_list.at[x,"Weighted Net"] = df.at[0, 'Weighted Net']
        ##########################################
        f.close()
        os.remove(today_file_name)
        print (str(x+1) +" of "+str(len(df_ticker_list))+" completed")

    except:
        print(df_ticker_list.at[x,"Stock Symbol"] + " fail")
        try:
            f.close()
            os.remove(today_file_name)
            f_temp.close()
            os.remove(today_file_name_temp)
        except:
            None
    ##########################################################
df_ticker_list.to_csv("All_Stock_list.csv", index=False)
