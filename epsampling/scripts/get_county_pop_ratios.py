'''
Start date: Sept 24, 2024
'''

import pandas as pd
from datetime import datetime
import sys
sys.path.insert(0, '..')
sys.path.insert(0, '../..')
sys.path.insert(0, '../epsampling/')

from epsampling.utils import load_latest_csv

date = datetime.today().strftime('%Y%m%d-%H%M%S')
data_dir = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'

df_pop,_ = load_latest_csv('all_county_acs_covs')
df_pop = df_pop[['State','State_fips','Fips','POP_x2']]
df_pop.rename({'POP_x2':'Pop'},axis=1,inplace=True)

states = list(df_pop.State.unique())

df_list = []
for state in states:
    df = df_pop[df_pop.State==state]
    
    tot_pop = sum(df.Pop)
    
    df['State_pop'] = tot_pop    
    df['Pop_ratio'] = df.apply(lambda x: round((x.Pop/x.State_pop),5), axis=1)
    df['Fips'] = df['Fips'].astype('str')
    
    df_list.append(df)
    
final_df = pd.concat(df_list)

final_df.to_csv(f'{data_dir}processed/pop_ratios_per_county_{date}.csv',index=False)