# > > pip install pandas censusdata requests

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NEED TO PROOF-READ !!!! # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import pandas as pd
import censusdata
from censusdata import Census

# Define API key and initialize Census object
api_key = "bcf88461fe70a6fd6b75262dcc087d1272228eef"
census = Census(api_key)

# Define parameters
state_ab = "NC"
year = 2021
geog = "county"
dat = "acs5"

# Define a function to fetch and process ACS data
def fetch_and_process_acs_data(table_id):
    data = censusdata.download("acs5", year, tables=[table_id], geo=censusdata.censusgeo([f"us:{state_ab}"]))
    df = pd.DataFrame(data)
    df = df.rename(columns={'NAME': 'NAME', 'variable': 'variable', 'estimate': 'estimate'})
    df = df.pivot(index='GEOID', columns='variable', values='estimate').reset_index()
    return df

# Fetch Age table
age_df = fetch_and_process_acs_data("B01001")
age_df = age_df.drop(columns=['NAME'])  # Drop NAME
age_df = age_df.rename(columns={
    'B01001_001': 'POP',
    'B01001_002': 'POP_M',
    'B01001_026': 'POP_F'
})

# Combine Male and Female populations
for i in range(3, 27):
    age_df.iloc[:, i] = age_df.iloc[:, i] + age_df.iloc[:, i + 26]

# Rename columns
age_columns = ["0004", "0509", "1014", "1517", "1819", "20", "21", "2224", "2529", "3034", "3539", "4044", "4549", "5054", "5559", "6061", "6264", "6566", "6769", "7074", "7579", "8084", "85p"]
age_df.columns = ['GEOID', 'POP', 'POP_M', 'POP_F'] + [f'POP_A{age}' for age in age_columns]

# Reorder columns
age_df = age_df[['GEOID', 'POP', 'POP_M', 'POP_F'] + [col for col in age_df.columns if col.startswith('POP_A')]]

# Write out CSV
age_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_age_sex.csv", index=False)

# Fetch Race/Ethnicity table
race_df = fetch_and_process_acs_data("B03002")
race_df = race_df.drop(columns=['NAME'])
race_df = race_df.rename(columns={
    'B03002_001': 'POP',
    'B03002_002': 'POP_NH',
    'B03002_013': 'POP_HISP'
})

# Create Race/Ethnicity tables
eth_only_df = race_df[['GEOID', 'POP', 'POP_HISP']]
race_eth_df = race_df.copy()
race_eth_df['B03002_008'] = race_eth_df['B03002_008'] + race_eth_df['B03002_009']
race_eth_df = race_eth_df.rename(columns={
    'B03002_003': 'POP_WHITE',
    'B03002_004': 'POP_BLACK',
    'B03002_005': 'POP_AIAN',
    'B03002_006': 'POP_ASIAN',
    'B03002_007': 'POP_NHPI',
    'B03002_008': 'POP_OTH2PL'
})

# Subset and reorder
race_eth_df = race_eth_df[['GEOID', 'POP', 'POP_NH', 'POP_HISP', 'POP_WHITE', 'POP_BLACK', 'POP_AIAN', 'POP_ASIAN', 'POP_NHPI', 'POP_OTH2PL']]

# Write out CSV files
eth_only_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_ethnicity.csv", index=False)
race_eth_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_race_ethnicity.csv", index=False)

# Fetch Income table
inc_df = fetch_and_process_acs_data("B19001")
inc_df = inc_df.rename(columns={'B19001_001': 'HH'})
inc_df.columns = ['GEOID'] + [f'HHI_{i}' for i in range(1, 16)]

# Write out CSV
inc_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_income.csv", index=False)

# Fetch Median Income table
med_inc_df = fetch_and_process_acs_data("B19013")
med_inc_df = med_inc_df.rename(columns={'B19013_001': 'MHI'})
med_inc_df = med_inc_df.merge(inc_df, on='GEOID')

# Write out CSV
med_inc_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_income.csv", index=False)

# Fetch Education table
edu_df = fetch_and_process_acs_data("B15003")
edu_df = edu_df.rename(columns={'B15003_001': 'POP_A25p'})
edu_df['EDU_ltHS'] = edu_df.loc[:, 'B15003_002':'B15003_017'].sum(axis=1)
edu_df['EDU_HS'] = edu_df.loc[:, 'B15003_018':'B15003_021'].sum(axis=1)

# Rename columns and subset
edu_df.columns = ['GEOID', 'POP_A25p', 'EDU_ltHS', 'EDU_HS'] + [f'EDU_{col}' for col in range(1, 6)]
edu_df = edu_df[['GEOID', 'POP_A25p', 'EDU_ltHS', 'EDU_HS'] + [col for col in edu_df.columns if col.startswith('EDU_')]]

# Write out CSV
edu_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_education.csv", index=False)

# Fetch Health Insurance table
hi_df = fetch_and_process_acs_data("B27010")
hi_df['HINS_A0018'] = hi_df['B27010_001'] - hi_df['B27010_017']
hi_df['HINS_A1934'] = hi_df['B27010_002'] - hi_df['B27010_033']
hi_df['HINS_A3564'] = hi_df['B27010_003'] - hi_df['B27010_050']
hi_df['HINS_A65p'] = hi_df['B27010_004'] - hi_df['B27010_066']

# Subset and reorder
hi_df = hi_df[['GEOID', 'B27010_001', 'B27010_002', 'B27010_003', 'B27010_004', 'HINS_A0018', 'HINS_A1934', 'HINS_A3564', 'HINS_A65p']]

# Write out CSV
hi_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_healthinsurance.csv", index=False)

# Fetch Occupancy Table
occ_df = fetch_and_process_acs_data("B25002")
occ_df = occ_df.rename(columns={'B25002_001': 'HU', 'B25002_002': 'HU_OCC', 'B25002_003': 'HU_VAC'})

# Fetch Own/Rent Table
occ2_df = fetch_and_process_acs_data("B25003")
occ2_df = occ2_df.rename(columns={'B25003_001': 'HU_OCC_OWN', 'B25003_002': 'HU_OCC_RENT'})

# Fetch Occupants per room Table
occ3_df = fetch_and_process_acs_data("B25014")
occ3_df['HU_OCC_OPRlt050'] = occ3_df['B25014_003'] + occ3_df['B25014_009']
occ3_df['HU_OCC_OPR051100'] = occ3_df['B25014_004'] + occ3_df['B25014_010']
occ3_df['HU_OCC_OPR101150'] = occ3_df['B25014_005'] + occ3_df['B25014_011']
occ3_df['HU_OCC_OPR151200'] = occ3_df['B25014_006'] + occ3_df['B25014_012']
occ3_df['HU_OCC_OPR200'] = occ3_df['B25014_007'] + occ3_df['B25014_013']
occ3_df = occ3_df[['GEOID', 'HU_OCC_OPRlt050', 'HU_OCC_OPR051100', 'HU_OCC_OPR101150', 'HU_OCC_OPR151200', 'HU_OCC_OPR200']]

# Write out CSV
occ3_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_occupancy.csv", index=False)

# Fetch Vehicles table
veh_df = fetch_and_process_acs_data("B25044")
veh_df = veh_df.rename(columns={'B25044_001': 'VEH'})

# Write out CSV
veh_df.to_csv(f"../data/acs_{year}/{state_ab}_{geog.replace(' ', '')}_{dat}_{year}_vehicles.csv", index=False)


import pandas as pd
from pandas.api.types import CategoricalDtype

# Load your datasets
# occ = pd.read_csv('path_to_occ.csv')
# occ2 = pd.read_csv('path_to_occ2.csv')
# occ3 = pd.read_csv('path_to_occ3.csv')
# hh = get_acs(...)
# hu = get_acs(...)
# emp = get_acs(...)
# ind = get_acs(...)

# Calculate values
occ3['HU_OCC_OPRlt050'] = occ3['B25014_003'] + occ3['B25014_009']
occ3['HU_OCC_OPR051100'] = occ3['B25014_004'] + occ3['B25014_010']
occ3['HU_OCC_OPR101150'] = occ3['B25014_005'] + occ3['B25014_011']
occ3['HU_OCC_OPR151200'] = occ3['B25014_006'] + occ3['B25014_012']
occ3['HU_OCC_OPRgt200'] = occ3['B25014_007'] + occ3['B25014_013']

# Merge (and subset)
occ = pd.merge(occ, occ2[['GEOID', 'Column3', 'Column4']], on='GEOID')
occ = pd.merge(occ, occ3[['GEOID', 'HU_OCC_OPRlt050', 'HU_OCC_OPR051100', 'HU_OCC_OPR101150', 'HU_OCC_OPR151200', 'HU_OCC_OPRgt200']], on='GEOID')

# Write out file
occ.to_csv(f"../data/acs_{yr}/{st_ab}_{geog.replace(' ', '')}_{dat}_{yr}_occupancy.csv", index=False)

# Retrieve Household Chars Table
hh = get_acs(survey=dat, year=yr, geography=geog, state=st_ab, table="B11016")

# Drop NAME, MOE
hh = hh[['GEOID', 'variable', 'estimate']]

# Convert to wide
hh = hh.pivot(index='GEOID', columns='variable', values='estimate').reset_index()

# Rename columns and calculate values
hh.columns = ['GEOID'] + list(hh.columns[1:])
hh.rename(columns={'B11016_001': 'HH', 'B11016_010': 'HH_1P'}, inplace=True)
hh['HH_2P'] = hh['B11016_003'] + hh['B11016_011']
hh['HH_3P'] = hh['B11016_004'] + hh['B11016_012']
hh['HH_4P'] = hh['B11016_005'] + hh['B11016_013']
hh['HH_5P'] = hh['B11016_006'] + hh['B11016_014']
hh['HH_6P'] = hh['B11016_007'] + hh['B11016_015']
hh['HH_7pP'] = hh['B11016_008'] + hh['B11016_016']

# Subset
hh = hh[['GEOID', 'HH', 'HH_1P', 'HH_2P', 'HH_3P', 'HH_4P', 'HH_5P', 'HH_6P', 'HH_7pP']]

# Write out file
hh.to_csv(f"../data/acs_{yr}/{st_ab}_{geog.replace(' ', '')}_{dat}_{yr}_householdsize.csv", index=False)

# Retrieve Structures Table
hu = get_acs(survey=dat, year=yr, geography=geog, state=st_ab, table="B25024")

# Drop NAME, MOE
hu = hu[['GEOID', 'variable', 'estimate']]

# Convert to wide
hu = hu.pivot(index='GEOID', columns='variable', values='estimate').reset_index()

# Rename columns and calculate values
hu.columns = ['GEOID'] + list(hu.columns[1:])
hu.rename(columns={'B25024_001': 'HU', 'B25024_010': 'HU_UISOTHER'}, inplace=True)
hu['HU_UISOTHER'] = hu['B25024_010'] + hu['B25024_011']

# Subset
hu = hu[['GEOID', 'HU', 'HU_UIS01D', 'HU_UIS01A', 'HU_UIS02', 'HU_UIS0304', 'HU_UIS0509', 'HU_UIS1019', 'HU_UIS2049', 'HU_UIS50P', 'HU_UISOTHER']]

# Write out file
hu.to_csv(f"../data/acs_{yr}/{st_ab}_{geog.replace(' ', '')}_{dat}_{yr}_unitsinstructure.csv", index=False)

# Retrieve Occupation/Employment Table
emp = get_acs(survey=dat, year=yr, geography=geog, state=st_ab, table="C24010")

# Drop NAME, MOE
emp = emp[['GEOID', 'variable', 'estimate']]

# Convert to wide
emp = emp.pivot(index='GEOID', columns='variable', values='estimate').reset_index()

# Consolidate M/F columns, subset
emp.iloc[:, 3:38] += emp.iloc[:, 40:75].values
emp = emp.drop(columns=emp.columns[39:75].tolist())

# Subset and rename first table
emp_1 = emp[['GEOID', 'POP_16p_EMP', 'OCC_MBSA', 'OCC_SERV', 'OCC_SALES', 'OCC_NRCM', 'OCC_PTMM']]
emp_1.columns = ['GEOID', 'POP_16p_EMP', 'OCC_MBSA', 'OCC_SERV', 'OCC_SALES', 'OCC_NRCM', 'OCC_PTMM']

# Write out file
emp_1.to_csv(f"../data/acs_{yr}/{st_ab}_{geog.replace(' ', '')}_{dat}_{yr}_occupation_level1.csv", index=False)

# Subset and rename second table
emp_2 = emp[['GEOID', 'POP_16p_EMP', 'OCC_MBSA_MBF', 'OCC_MBSA_CES', 'OCC_MBSA_ELCAM', 'OCC_MBSA_HCPT', 'OCC_SERV_HCS', 'OCC_SERV_PS', 'OCC_SERV_FPS', 'OCC_SERV_BGM', 'OCC_SERV_PCS', 'OCC_SALES_SR', 'OCC_SALES_OAS', 'OCC_NRCM_FFF', 'OCC_NRCM_CE', 'OCC_NRCM_IMR', 'OCC_PTMM_P', 'OCC_PTMM_T', 'OCC_PTMM_MM']]
emp_2.columns = ['GEOID', 'POP_16p_EMP', 'OCC_MBSA_MBF', 'OCC_MBSA_CES', 'OCC_MBSA_ELCAM', 'OCC_MBSA_HCPT', 'OCC_SERV_HCS', 'OCC_SERV_PS', 'OCC_SERV_FPS', 'OCC_SERV_BGM', 'OCC_SERV_PCS', 'OCC_SALES_SR', 'OCC_SALES_OAS', 'OCC_NRCM_FFF', 'OCC_NRCM_CE', 'OCC_NRCM_IMR', 'OCC_PTMM_P', 'OCC_PTMM_T', 'OCC_PTMM_MM']

# Write out file
emp_2.to_csv(f"../data/acs_{yr}/{st_ab}_{geog.replace(' ', '')}_{dat}_{yr}_occupation_level2.csv", index=False)

# Subset and rename third table
emp_3 = emp[['GEOID', 'POP_16p_EMP', 'OCC_MBSA_MBF_M', 'OCC_MBSA_MBF_BF', 'OCC_MBSA_CES_CM', 'OCC_MBSA_CES_AE', 'OCC_MBSA_CES_LPSS', 'OCC_MBSA_ELCAM_CSS', 'OCC_MBSA_ELCAM_L', 'OCC_MBSA_ELCAM_EL', 'OCC_MBSA_ELCAM_ADESM', 'OCC_MBSA_HCPT_HDTP', 'OCC_MBSA_HCPT_HTT', 'OCC_SERV_HCS', 'OCC_SERV_PS_FP', 'OCC_SERV_PS_LE', 'OCC_SERV_FPS', 'OCC_SERV_BGM', 'OCC_SERV_PCS', 'OCC_SALES_SR', 'OCC_SALES_OAS', 'OCC_NRCM_FFF', 'OCC_NRCM_CE', 'OCC_NRCM_IMR', 'OCC_PTMM_P', 'OCC_PTMM_T', 'OCC_PTMM_MM']]
emp_3.columns = ['GEOID', 'POP_16p_EMP', 'OCC_MBSA_MBF_M', 'OCC_MBSA_MBF_BF', 'OCC_MBSA_CES_CM', 'OCC_MBSA_CES_AE', 'OCC_MBSA_CES_LPSS', 'OCC_MBSA_ELCAM_CSS', 'OCC_MBSA_ELCAM_L', 'OCC_MBSA_ELCAM
