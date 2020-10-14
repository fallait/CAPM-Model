from __future__ import division  
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.morningstar import Fundamentals
from quantopian.pipeline.filters import Q1500US
from quantopian.pipeline.factors import CustomFactor, DailyReturns
from quantopian.pipeline.factors import SimpleBeta
import quantopian.algorithm as algo
import numpy as np
import pandas as pd
import re

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every month, 1 hour after market open.
    algo.schedule_function(
        before_trading_start,
        algo.date_rules.month_start(),
        algo.time_rules.market_open(hours=1),
    )

    algo.schedule_function(
        rebalance,
        algo.date_rules.month_start(),   
        algo.time_rules.market_open(hours=1),
    )
    
    #algo.schedule_function(
     #   cover,
     #   algo.date_rules.month_start(),   
     #   algo.time_rules.market_open(hours=1),
   # )
    
   
    
    #algo.schedule_function(
    #    cover,
    #    algo.date_rules.month_end(),
    #    algo.time_rules.market_open(hours=1),
   #)
    
    
    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(context), 'pipeline')    
# gives price of equity one month ago
class Factor_N_Days_Ago(CustomFactor):  
    def compute(self, today, assets, out, input_factor):  
        out[:] = input_factor[0]
        
# gives price of SPY one month ago
class SPYReturnLastMonth(CustomFactor):  
    inputs = [USEquityPricing.close]  
    window_length = 22  
    def compute(self, today, assets, out, close):  
        out[:] = (close[-1][np.where(assets==8554)]- close[-21][np.where(assets==8554)])/close[-21][np.where(assets==8554)]

def make_pipeline(context):
    
    # Base universe set to the QTradableStocksUS
    base_universe = Q1500US()

    # Factor of yesterday's close price.
    yesterday_close = USEquityPricing.close.latest
    lastmonth_close = Factor_N_Days_Ago(inputs = [USEquityPricing.close], window_length = 22)
    
    marketReturnLastMonth = SPYReturnLastMonth()
    predictedMarketReturn = marketReturnLastMonth
    riskFree = .15
    
    sector = Fundamentals.morningstar_sector_code.latest
    
    beta = SimpleBeta(symbol('SPY'),regression_length=252)
    
    capmReturnLastMonth = riskFree + SimpleBeta(symbol('SPY'),regression_length=252) * (marketReturnLastMonth - riskFree)
    
    capmReturnThisMonth = riskFree + SimpleBeta(symbol('SPY'),regression_length=252) * (predictedMarketReturn - riskFree)
    
    stockPriceShouldBe = lastmonth_close + (lastmonth_close * capmReturnLastMonth * capmReturnThisMonth)
    
    mcap = morningstar.valuation.market_cap.latest  
    
    target_sectors = [101, 205, 308, 310, 311]
    
    sector_filter = sector.element_of(target_sectors)

    pipe = Pipeline(
        columns={
            'close': yesterday_close,
            'lagged': lastmonth_close,
            'sector' : sector,
            'beta' : beta,
            'market cap' : mcap,
            'capmReturnLastMonth' : capmReturnLastMonth,
            'capmReturnThisMonth' : capmReturnThisMonth,
            'stockPriceShouldBe' : stockPriceShouldBe
        },
        
        screen=((Q1500US()) & (mcap > 10000000000) & (sector_filter) & (.6 <= beta) & (1.2 >= beta))
    )
    return pipe

def side(context):
    df = context.output.copy()
    todrop = ['lagged', 'market cap', 'capmReturnLastMonth', 'capmReturnThisMonth']
   # for df in results:
    df['projectedUpside'] = ''
    i=0
    for row in df.itertuples():
            #print(row)
            #Date = 0, beta = 1, capmReturnLastMonth = 2, capmReturnThisMonth = 3, close = 4,
            #lagged = 5, mcap = 6, sector = 7, stockPriceShouldBe = 8, projectedUpside = 9
        df.iat[i, 8] = (row[8] - row[4]) / row[4]
        #if ((row[8] - row[4])) > 0:
         #   df.iat[i, 8] = (row[8] - row[4])/row[4]
          #  df.iat[i, 9] = 0
        #elif ((row[8] - row[4])) < 0:
         #   df.iat[i, 9] = (row[8] - row[4])/row[4]
          #  df.iat[i, 8] = 0
        i+=1
    df.drop(todrop, axis=1, inplace = True)
        # new_df = df.drop(df[df.projectedUpside > .04].index)
    df.drop(df[df.projectedUpside > .04].index, inplace=True)
    df.drop(df[df.projectedUpside < -.04].index, inplace=True)
    sectorlist=[x for _, x in df.groupby(df['sector'])]
    #indexNames = df[(df['projectedUpside'] > 0.04) | (df['projectedDownside' < -.04])].index
    #df.drop(indexNames, inplace=True)
    return sectorlist

def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = algo.pipeline_output('pipeline')    
    # These are the securities that we are interested in trading each day.
    something = side(context)
    selected = selection(something)
    context.results = positions(selected)
    
    context.security_list, context.orders, context.spy = context.results
    #print(context.security_list)
    #print(context.orders)
    #print(context.spy)
    
    
    #context.security_list = context.output.index

def selection(sectorlist):
    selectlist = []
    for sector in sectorlist:
       # print(sector['projectedUpside'].idxmax())
       # print(sector['projectedUpside'].idxmin())
        l = sector.loc[sector['projectedUpside'].idxmax()]
        s = sector.loc[sector['projectedUpside'].idxmin()]
        selectlist.append(l)
        selectlist.append(s)
    #selectlist[0][0] = beta of first long
    #selectlist[0][4] = upside (positive if long, negative if short)
    return selectlist


def positions(selectlist):
    weightNumerator = 100 / (len(selectlist))
    totalequity = 0
    totalspy = 0
    equitylist = []
    poslist = np.array([])
    for selection in selectlist:
        percentBuySellStock = weightNumerator/(1+selection[0])
        percentBuySellMarket = weightNumerator - percentBuySellStock
        sid = selection.name
        equitylist.append(sid)
        if selection[4] >= 0:
            percentBuySellMarket = (-percentBuySellMarket)
        else:
            percentBuySellStock = (-percentBuySellStock)
        poslist = np.append(poslist, percentBuySellStock)
        #print(str(sid) + ' Stock= ' + str(percentBuySellStock) + ' Market= ' + str(percentBuySellMarket))
        totalequity += abs(percentBuySellStock)
        totalspy += percentBuySellMarket
        totalExposure = totalequity + totalspy
    multiplier = 100/totalExposure
    poslist = poslist * multiplier
    totalspy = totalspy * multiplier
    return equitylist, poslist/100, totalspy/100

def rebalance(context, data):
    #print(context.security_list[0])
    #for position in (context.portfolio.positions):
        #order_target_percent(position, 0)
    #order_target_percent(symbol('SPY'), 0)
    for position in (context.portfolio.positions):
        if position != symbol('SPY'):
            order_target_percent(position, 0)
    order_target_percent(symbol('SPY'), context.spy)
    for i in range(0, len(context.security_list)):
        #if not bool(get_open_orders(context.security_list[i])):
        if context.security_list[i] not in get_open_orders() and data.can_trade(context.security_list[i]) and context.account.leverage < 1.2:
            order_target_percent(context.security_list[i], context.orders[i])
        #order_target_percent(symbol('SPY'), context.spy)

#def cover(context, data):
    #log.info( [ str(s.symbol) for s in sorted(context.portfolio.positions) ] )
    #for position in (context.portfolio.positions):
        #order_target_percent(position, 0)
    #order_target_percent(symbol('SPY'), 0)