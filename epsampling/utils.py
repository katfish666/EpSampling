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
def load_csv(csv_name, data_dir=f'{DATA_DIR}processed/', timestamp=''):
    if timestamp:
        latest = timestamp
    else:
        dates = [x[-19:-4] for x in glob.glob(f'{data_dir}{csv_name}*')]
        dates = sorted([x for x in dates if x[:3]=='202'])
        latest = dates[-1]
    file_name = f'{data_dir}{csv_name}_{latest}.csv'
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