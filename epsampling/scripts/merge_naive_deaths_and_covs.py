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
from tqdm import tqdm

df_acs,_ = load_latest_csv('all_county_acs_covs')
df_deaths,_ = load_latest_csv('naive_deaths_all_counties')

fips_acs = list(df_acs.Fips.unique())
fips_deaths = list(df_deaths.Fips.unique())
fips_set = list(set(fips_acs) & set(fips_deaths))
fips_set = [x for x in fips_set if x!='State']

dfs_full = []

for fips in tqdm(fips_set,total=len(fips_set)):
    
    subdf_acs = df_acs[df_acs.Fips==fips]
    subdf_deaths = df_deaths[df_deaths.Fips==fips]
    
    for col in subdf_acs.columns:
        subdf_deaths[col] = subdf_acs[col].values[0]
        
    dfs_full.append(subdf_deaths)
    
final_df = pd.concat(dfs_full)

data_dir = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'
d = datetime.today().strftime('%Y%m%d-%H%M%S')
final_df.to_csv(f'{data_dir}processed/merged_covs_deaths_{d}.csv',index=False)