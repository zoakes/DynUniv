import pandas_datareader as pdr 
import pandas as pd 

import quandl


def get_all_symbols(to_list=True):
    '''Get all NASDAQ sybols'''
    from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
    symbols = get_nasdaq_symbols()
    symbols = symbols.reset_index()
    symbols = symbols['Symbol']
    if to_list:
        return symbols.tolist()
    return symbols



'''Coarse Universe Selection'''


def simple_pipe(df,condition_str,collist=None):
    '''Very simple version -- using query function in pd'''
    df = df.query(condition_str)
    if collist is not None:
        collist.append('ticker')
        df = df[collist].set_index('ticker')
    return df



'''Fine Selection -- Pipeline : Dictionary + Function '''

nest_dict = { 
    'FILT': {
        'cond1':df['workingcapital'] > 5000000,  
        'cond2':df['sps'] > 50,
        #'cond3':df['currentratio'] > 1.5, #top_25, -- used this in base_univ instead.
    },
    'COLS': { 
        'col1' : 'workingcapital',
        'col2': 'ticker',
        'col3':'sps',
    },
    
    'INDIC': { 
        'm1': df['currentratio'].quantile(.50),
        'tst': df['currentratio'] > nest_dict['INDIC']['m1'],
        't2': df['de'].mean()
        
    },
    'BASE':base_universe
}


def make_pipe(pipe_dict,ret_all=True):
    '''
    Create DF using pipeline dict -- 
    if ret_all is false, returns only explicit columsn in COLS
    '''
    f = nest_dict['FILT']
    c = nest_dict['COLS']
    ic = nest_dict['INDIC']
    df = base_universe
    
    #Add indicators
    for k,v in ic.items(): 
        df[str(k)] = v
        
    #Try filters? Think these worked?!
    for k, v in f.items():
        df = df[v]
        
    if ret_all is False:    
        cols = [i for i in c.values()]
        df = df[cols]
        df = df.set_index('ticker') 
    return df






if __name__ == '__main__':
    '''Testing Quandl'''
    quandl.ApiConfig.api_key = 'bNhmL6hk7a3boYd7Rbfz' #YOUR_API_KEY_HERE
    quandl.get_table('SHARADAR/SF1', calendardate='2018-12-31,2017-12-31,2016-12-31', ticker='XOM,WMT,VZ')

    slst = get_all_symbols(to_list=True)

    universe = quandl.get_table('SHARADAR/SF1',calendardate='2018-12-31',ticker=slst)

    simple_pipe(universe,'workingcapital > 50000000 & sps > 50',['workingcapital','sps'])


    '''Fine Selection'''

    #Functional Method 
    df = universe #key! don't forget this.

    top_25 = df['currentratio'] >= (df['currentratio'].quantile(.75))
    btm_25 = df['currentratio'] <= (df['currentratio'].quantile(.25))

    top_btm = df[(top_25 | btm_25)]

    base_universe =  top_btm

    #OOP Method 

    spr = simple_pipe(df,'currentratio >= currentratio.quantile(.75) | currentratio <= currentratio.quantile(.25)')
    base_universe = spr 

    make_pipe(nest_dict,False)


