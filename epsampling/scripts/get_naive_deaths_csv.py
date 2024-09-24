'''
Start date: Sept 24, 2024
'''

import pandas as pd
from datetime import datetime
import sys
from tqdm import tqdm
sys.path.insert(0, '..')
sys.path.insert(0, '../..')
sys.path.insert(0, '../epsampling/')

from epsampling.utils import load_latest_csv

data_dir = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'


from epsampling.utils import load_latest_csv

#############################################
### Get state COVIDhub-ensemble predictions
#############################################

## Get fips
state_to_fips = pd.read_csv('../../constants/state_fips.csv')
state_to_fips.rename({'FIPS':'State_fips'},axis=1,inplace=True)

## Add fips to forecast table.
df,_ = load_latest_csv('covidhub_ensemble_1wkcum_point')
df.columns = df.columns.str.capitalize()
df.rename({'Location':'State_fips'},axis=1,inplace=True)

df = df.merge(state_to_fips, on='State_fips')

## Remove and rename columns ...
df_states = df.drop(['Target','Forecast_date'], axis=1, errors='ignore')
df_states.rename({'Target_end_date':'Date', 'Value':'COVIDhubEns_state_deaths'}, 
                 axis=1, inplace=True)
# df_states


#############################################
### Get ground truth covid deaths per county.
#############################################

df_counties = pd.read_csv(f'{data_dir}nytimes/us-counties.csv')
df_counties.columns = df_counties.columns.str.capitalize()
df_counties.drop(['Cases'],axis=1,inplace=True)
df_counties.dropna(inplace=True)

## Make list of dfs because everything in one df is too big.
forecast_dates = list(df_states.Date.unique())
all_states = list(df_counties.State.unique())

state_dfs = {}

for state in tqdm(all_states, total=len(all_states), desc='Make df per state'):
#     if state in ['Virgin Islands','Northern Mariana Islands']:
#         continue
    df = df_counties[df_counties.State==state]
    df['Fips'] = df['Fips'].astype('int64').astype('str')
    ## Only need dates for counties that we have for states ... 
    df = df[df.Date.isin(forecast_dates)]
    state_dfs[state] = df
    
    
#############################################
### Get pop ratios for each county.
#############################################

df_pop,_ = load_latest_csv('pop_ratios_per_county',f'{data_dir}processed/')
df_pop['Fips'] = df_pop['Fips'].astype('int64').astype('str')
df_pop = df_pop[['Postal','Fips', 'Pop', 'State', 'Pop_ratio']]

for state,df in state_dfs.items():
    df_state_pop = df_pop[df_pop.State==state]    
    df = df.merge(df_state_pop, on=['Fips','State'])
    state_dfs[state] = df


#############################################
### Compute naive deaths for each county.
#############################################

merged_dfs = {}
for state in tqdm(state_dfs.keys(), total=len(state_dfs), desc='Compute naive deaths'):
    
    df_state = df_states[df_states.State==state]
    df_counties = state_dfs[state]
    
#     print(df_state.columns, df_counties.columns)
#     display(df_counties, df_state)
    df_merged = df_counties.merge(df_state, on=['Date','State','Postal'])
    df_merged.rename({'Deaths':'True_county_deaths'}, axis=1, inplace=True)
    
    df_merged['Naive_county_deaths'] = df_merged.apply(lambda x: 
                                                       x.Pop_ratio * x.COVIDhubEns_state_deaths, 
                                                       axis=1)
    
    df_merged = df_merged[['State_fips','State','Postal','County',
                           'Fips','Date','COVIDhubEns_state_deaths',
                           'Pop','Pop_ratio','True_county_deaths',
                           'Naive_county_deaths']]
    merged_dfs[state] = df_merged
    
    
#############################################    
### Save dataframe.
#############################################    

final_df = pd.concat(merged_dfs.values())

data_dir = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'
d = datetime.today().strftime('%Y%m%d-%H%M%S')
final_df.to_csv(f'{data_dir}processed/naive_deaths_all_counties_{d}.csv',index=False)