import pandas as pd
import numpy as np
import csv
from tqdm import tqdm
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


DATE = datetime.today().strftime('%Y%m%d-%H%M%S')
DATA_DIR = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'

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
#     print(y,m,d)
#     return 10*y + 100*m + d


from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sktime.performance_metrics.forecasting import mean_relative_absolute_error
def get_performance(model_names, model_preds, y_test, y_naive):
    
    metrics_dict = {'MAE': mean_absolute_error,
                    'MSE': mean_squared_error,
                    'r2': r2_score,
                    'relMAE': mean_relative_absolute_error}

    model_res_dict = {model:{} for model in model_names}
    
    for model,pred in zip(model_names, model_preds):
        for metric, func in metrics_dict.items():
            if metric=='relMAE':
                model_res_dict[model][metric] = func(y_test, pred, y_pred_benchmark=y_naive)
            else:
                model_res_dict[model][metric] = func(y_test, pred)
            
    return model_res_dict


def drop_duplicate_cols(df):
    uniq, idxs = np.unique(df, return_index=True, axis=1)
    return pd.DataFrame(uniq, index=df.index, columns=df.columns[idxs])


def drop_sers_with_nans(df, from_axis='rows'):
    
    if df.isnull().values.any()==True:
        print(f'Dropped {from_axis} with NaNs!')

        if from_axis=='rows':
            dff = df.dropna(axis=0, inplace=False)
            dff.reset_index(drop=True, inplace=True)
            print(f'Num rows before: {df.shape[0]}')
            print(f'Num rows after: {dff.shape[0]}')
            
        elif from_axis=='cols':
            dff = df.dropna(axis=1, inplace=False)
            print(f'Num cols before: {(df.shape[1])}')
            print(f'Num cols after: {(dff.shape[1])}')
            
        return dff
    
    else:
        print(f'No NaNs! :)')
        return df