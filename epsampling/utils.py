import pandas as pd
import numpy as np
import csv
from tqdm import tqdm
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


DATE = datetime.today().strftime('%Y%m%d-%H%M%S')
DATA_DIR = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'


def printt(string, print_out=False):
    if print_out: print(string)
    else: return
    
import glob
import os

def drop_duplicate_cols(df):
    uniq, idxs = np.unique(df, return_index=True, axis=1)
    return pd.DataFrame(uniq, index=df.index, columns=df.columns[idxs])

def date_str_to_int(date):
    stripped = ("").join(date.split('-'))
    num = int(stripped)
    return num

def fips_int_to_str(df):
    if 'State_fips' in df.columns:
        df['State_fips'] = df.State_fips.astype('str')
        df['State_fips'] = df.State_fips.apply(lambda x: x.zfill(2))
    if 'Fips' in df.columns:
        df['Fips'] = df.Fips.astype('str')
        df['Fips'] = df.Fips.apply(lambda x: x.zfill(5))
    return df


def load_csv(csv_name, path=f'{DATA_DIR}processed/', timestamp=''):
# def load_csv(csv_name, path=os.path.join(DATA_DIR,'processed'), timestamp=''):

    if timestamp:
        latest = timestamp
    else:
        files = glob.glob(f'{path}/{csv_name}*')
        dates = [x[-19:-4] for x in files]
        dates = sorted([x for x in dates if x[:3]=='202'])
        latest = dates[-1]
    file_name = f'{path}{csv_name}_{latest}.csv'
    df = pd.read_csv(file_name)
    return df, file_name

def time_to_int(date):
    y,m,d = date.split('-')
    total = 0
    total += int(d) * 60 
    total += (int(m) - 1) * 60 * 60 
    total += (int(y) - 1970) * 60 * 60 * 24
    return total


def drop_duplicate_cols(df):
    uniq, idxs = np.unique(df, return_index=True, axis=1)
    return pd.DataFrame(uniq, index=df.index, columns=df.columns[idxs])


def drop_sers_with_nans(df, from_axis='rows', print_out=False):
    if df.isnull().values.any()==True:
        printt(f"Dropped {from_axis} with NaNs!", print_out)
        if from_axis=='rows':
            dff = df.dropna(axis=0, inplace=False)
            dff.reset_index(drop=True, inplace=True)
            printt(f"Num rows before: {df.shape[0]}\n"
                   f"Num rows after: {dff.shape[0]}", print_out) 
        elif from_axis=='cols':
            dff = df.dropna(axis=1, inplace=False)
            printt(f"Num cols before: {df.shape[1]}\n"
                   f"Num cols after: {dff.shape[1]}", print_out) 
        return dff
    else:
        printt(f"No NaNs! :)", print_out)
        return df
    
def get_chunks(lst, num_membs=4):
    chunks = []
    for i in range(0, len(lst), num_membs):
        chunks.append(lst[i:i + num_membs])
    return chunks
