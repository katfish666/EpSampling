import glob
import os
import pandas as pd
import numpy as np

from datetime import datetime
from tqdm import tqdm

from epsampling.utils import drop_sers_with_nans, date_str_to_int, load_csv

# DATA_DIR = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'
DATA_DIR = '/work/users/k/4/k4thryn/Repos/OLD_EpSampling_Nov2024/data/'

DT = datetime.today().strftime('%Y%m%d-%H%M%S')

def load_modeling_df(timestamp, xform=False):
    ## TARGET DATA
    fpath = os.path.join(DATA_DIR,'processed',f'training_target_df_{timestamp}.csv')
    df = pd.read_csv(fpath)
    ## drop nans, drop negatives
    df.dropna(inplace=True)
    df = df[(df >= 0).all(axis=1)]
    
    ## ACS DATA 
    fpath = os.path.join(DATA_DIR,'processed',f'training_acs_df_{timestamp}.csv')
    df_acs = pd.read_csv(fpath)
    ## standardize, not on fips tho lol
    df_acs.set_index('Fips',inplace=True,drop=True)
    df_acs = (df_acs-df_acs.mean()) / df_acs.std()

    ## TRANSFORM
    if xform==True:
        county_transform = [x for x in df.columns if x.startswith('True_county_inc_deaths')]
        county_transform += ['Naive_proj_deaths','Naive_true_deaths']
        for var in county_transform:
            df[f'{var}_x'] = df.apply(lambda x: (x[var]) / x.Pop * 100000 , axis=1)
        state_transform = ['Proj_state_inc_deaths','True_state_inc_deaths']
        for var in state_transform:
            df[f'{var}_x'] = df.apply(lambda x: (x[var]) / x.State_pop * 100000, axis=1)
           
    df = df.merge(df_acs,on='Fips')    
    
    return df





# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# COVID19 Forecasting Hub Predictions data processing # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def get_hub_df(state_fips=None):
    '''
    params:
        state_fips (str) -> which state data to subset if desired. if not then
        all states' data are returned. NC state fips is '37'.
    returns:
        df (pd.DataFrame) -> dataframe of covidhub ensemble projections.
    '''
    my_dir = os.path.join(DATA_DIR,'raw','COVIDhub-ensemble')
    files = glob.glob(f'{my_dir}/*.csv')

    types = ['point']
    targets = ['1 wk ahead inc death']

    all_dfs = []
    for f in tqdm(files,total=len(files)):
        df = pd.read_csv(f)
        df = df[df.type=='point']
        df = df[df.target.isin(targets)]
        df = df[df.location!='US']
        all_dfs.append(df)
    df_all = pd.concat(all_dfs)
    
    df = df_all[['location','target_end_date','value']]    
    df.rename({'location':'State_fips',
               'target_end_date':'Date',
               'value':'Proj_state_inc_deaths'}, axis=1, inplace=True)

    df['State_fips'] = df.State_fips.astype(int)
    df['Date'] = df.Date.apply(lambda x: date_str_to_int(x))

    if state_fips is not None:
        df = df[df.State_fips==state_fips]
        
    # set to 32-bit
    df[df.select_dtypes(np.float64).columns] = df.select_dtypes(np.float64).astype(np.float32)
    df[df.select_dtypes(np.int64).columns] = df.select_dtypes(np.int64).astype(np.int32)
        
    df.reset_index(drop=True,inplace=True)
    df.sort_values(['State_fips','Date'], inplace=True)
    return df



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NYT reported deaths data processing # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def get_death_df(state_fips=None):
    
    '''
    params:
        state_fips (str) -> which state data to subset if desired. if not then
        all states' data are returned. NC state fips is '37'.
    returns:
        df (pd.DataFrame) -> dataframe of nyt reports per county.
    '''
    
    fpath = os.path.join(DATA_DIR,'raw','nytimes','us-counties.csv')
    df = pd.read_csv(fpath)

    df = drop_sers_with_nans(df, from_axis='rows', print_out=False)

    ## REFORMAT dataframe ...
    df.columns = df.columns.str.capitalize()
    df.rename({'Deaths':'True_county_cum_deaths'},axis=1,inplace=True)
    df = df[['Fips','Date', 'True_county_cum_deaths']]
    df['Fips'] = df.Fips.astype(int)

    ## Pull out samples from 'nytimes' that have matched dates to 'COVIDhub-ensemble' ...
    df['Date'] = df.Date.apply(lambda x: date_str_to_int(x))
    df_hub,_ = load_csv('formatted_COVIDhub-ensemble')
    my_dates = df_hub.Date.unique().tolist()
    df = df[df.Date.isin(my_dates)]

    ## only nc
    if state_fips is not None:
        df = df[df.Fips.astype(str).str.startswith(str(state_fips))]

    ## get county inc deaths
    dfs = []
    for fips in tqdm(df.Fips.unique()):

        df_county = df[df.Fips==fips]
        df_county.reset_index(inplace=True, drop=True)

        inc_deathss = []
        for i in range(len(df_county)):
            if i==0:
                inc_deaths = np.nan
            else:     
                inc_deaths = df_county.at[i,'True_county_cum_deaths'] - \
                df_county.at[i-1,'True_county_cum_deaths']   
            inc_deathss.append(inc_deaths)

        df_county['True_county_inc_deaths'] = inc_deathss
        dfs.append(df_county)

    df = pd.concat(dfs)
    
    # set to 32-bit
    df[df.select_dtypes(np.float64).columns] = df.select_dtypes(np.float64).astype(np.float32)
    df[df.select_dtypes(np.int64).columns] = df.select_dtypes(np.int64).astype(np.int32)
    
    df.reset_index(inplace=True,drop=True)
    return df



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Census data processing  # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def normalize_attrs_by_pop(df, f):
    f = (f[f.rindex('/')+1:-4])
    f = f[f.rindex('_')+1:]
    
    df.set_index('GEOID',drop=True,inplace=True)

    if f=='healthinsurance':
        df['HINS_A0018'] = df['HINS_A0018'].div(df['POP_A0018'])
        df['HINS_A1934'] = df['HINS_A1934'].div(df['POP_A1934'])
        df['HINS_A3564'] = df['HINS_A3564'].div(df['POP_A3564'])
        df['HINS_A65p'] = df['HINS_A65p'].div(df['POP_A65p'])
        
        dff = df.drop(['POP_A0018','POP_A1934','POP_A1934','POP_A65p'],inplace=False,axis=1)
        
    elif f=='income':
        denom = df['HH']
        
        dff = df.apply(lambda x: x/denom, axis=0) 
        ## fix MHI since its not supposed to be normalized
        dff['MHI'] = df['MHI']
        dff['HH'] = denom

    else:
        universe = df.columns[0]
        denom = df[universe]
        
        dff = df.apply(lambda x: x/denom, axis=0)
        dff[universe] = denom
        
    dff = dff.reset_index(inplace=False, drop=False)
    return dff


def get_state_df(files):
    first_df = None
    for i,f in enumerate(files):
        this_df = pd.read_csv(f)
        this_df = normalize_attrs_by_pop(this_df, f)
        if first_df is None:
            first_df = this_df
        else:
            df = pd.merge(first_df, this_df, on='GEOID', suffixes=(f'_x{i}', f'_x{i+1}'))
            first_df = df
    return df

    
def get_acs_df(state_fips=None):

    acs_dir = '/work/users/k/4/k4thryn/Repos/EpSampling/data/raw/acs_results/'

    all_st_dfs = []

    state_dirs = [x for x in os.walk(acs_dir)][0][1]

    for i,state in enumerate(state_dirs):
        if i==0:
            continue    

        files = glob.glob(f'{acs_dir}{state}/*.csv')
        df = get_state_df(files)
        ## REFORMAT dataframe ... rename cols.
        df.rename({'GEOID':'Fips'},axis=1,inplace=True)
        all_st_dfs.append(df)

    df = pd.concat(all_st_dfs)
    df.reset_index(drop=True,inplace=True)

    ## check for cols with nans.
    df = drop_sers_with_nans(df, from_axis='cols')
    
    # # # # # #
    # Add pop ratio and dedup identical covs.
    # # # # # #
    
    ## Rename cols
    df.rename({'POP_x2':'Pop'},axis=1,inplace=True)
    ## Reorder columns
    df = df[['Fips','Pop'] + [c for c in df.columns if c not in ['Fips','Pop']]]

    # # # # # # # # # # # # # # # # # # # # # # # #
    ## Get county ratios and insert state pop, state fips, and county ratio cols.
    # # # # # # # # # # # # # # # # # # # # # # # # 

    df.insert(2, 'State_fips', 0)
    df.insert(3, 'State_pop',0)
    df.insert(4, 'Ratio', 0)

    for tup in df.itertuples():

        st_fips = tup.Fips // 1000
        df.at[tup.Index, 'State_fips'] = st_fips    

    for tup in df.itertuples():

        state_pop = sum(df[df.State_fips==tup.State_fips].Pop)
        ratio = tup.Pop / state_pop

        df.at[tup.Index, 'State_pop'] = state_pop
        df.at[tup.Index, 'Ratio'] = ratio
        
        
    if state_fips is not None:
        df = df[df.State_fips==state_fips]

    df.reset_index(inplace=True, drop=True)    
    return df


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Misc data processing  # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def get_state_inc_deaths_col(df_hub, df_death):
    
    df = df_hub.copy()
    df.set_index('Date',drop=False,inplace=True)
    df['True_state_inc_deaths'] = 0
    
    for date in df.Date.unique():
        subdf = df_death[df_death.Date==date]
        inc_deaths = sum(subdf.True_county_inc_deaths)
        df.at[date, 'True_state_inc_deaths'] = inc_deaths

    return df


def get_naive_deaths_col(df, df_acs):
    ## Join with acs[[fips,pop,state_fips,state_pop,ratio]] ...
    df = df.merge(df_acs[['Fips','Pop','State_pop','Ratio']], on='Fips')
    ## Compute naive inc deaths.
    df['Naive_proj_deaths'] = df.apply(lambda x: x.Proj_state_inc_deaths * x.Ratio, axis=1)
    df['Naive_true_deaths'] = df.apply(lambda x: x.True_state_inc_deaths * x.Ratio, axis=1)
    ## Reorder columns
    df = df[['Date',  'State_fips','Fips', 'Pop', 'State_pop', 'Ratio', 
             'Proj_state_inc_deaths', 'True_state_inc_deaths',
             'True_county_cum_deaths', 'True_county_inc_deaths', 
             'Naive_proj_deaths', 'Naive_true_deaths']]
    return df



def get_full_target_df(df_hub, df_death, df_acs):
    
    df = get_state_inc_deaths_col(df_hub, df_death)    
    df.drop('Date',axis=1,inplace=True)
    
    df = df.merge(df_death,on='Date')
         
    df = get_naive_deaths_col(df, df_acs)
    
    df.dropna(inplace=True)
    df.reset_index(inplace=True, drop=True)
    
    return df


def get_only_acs_covs_df(df_acs):
    df = df_acs.drop(['State_fips','State_pop','Ratio'],axis=1)
    df.rename({'Pop':'POP'},axis=1,inplace=True)
    return df
    

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# History cols processing # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def add_death_history_cols(df, target_col, from_week=3, num_weeks=8):

    to_week = num_weeks + from_week

    dates = sorted(df.Date.unique())
    for i in range(from_week, to_week):
        df[f'{target_col}_tm_{i+1}'] = None

    fipss = df.Fips.unique()

    dfs = []
    for fips in tqdm(fipss,total=len(fipss)):
        df_county = df[df.Fips==fips] 
        df_county.sort_values('Date',inplace=True)
        df_county.reset_index(inplace=True,drop=True)

        for i in range(len(df_county)):

            if i < to_week:
                continue
            else:

                from_date = i - from_week 
                to_date = i - to_week 

                this = df_county[to_date:from_date][target_col].values[::-1]
                wk_to_death = { f'{target_col}_tm_{k+1}':v for (k,v) in zip(range(from_week,to_week),this) }
                df_county.loc[i, wk_to_death.keys()] = wk_to_death.values()

        dfs.append(df_county)
        
    df_tot = pd.concat(dfs)
    df_tot.reset_index(inplace=True,drop=True)
    
    return df_tot
