from setup_nb_env import *


## STATISTICAL MODELS # # # # # # # # #


from epsampling.utils import load_csv
# pd.set_option('display.float_format', lambda x: '%.3f' % x)
from epsampling.utils import drop_sers_with_nans
from epsampling.utils import date_str_to_int

DATA_DIR = '/work/users/k/4/k4thryn/Repos/EpSampling/data/'
DT = datetime.today().strftime('%Y%m%d-%H%M%S')

def get_full_modeling_dfs(timestamp='20241009-144131'):
    
    fpath = os.path.join(DATA_DIR,'processed', f'processed_naive_deaths_{timestamp}.csv')
    df_deaths = pd.read_csv(fpath)
    # display(df)

    ## Drop rows with nans.
    df_deaths.dropna(inplace=True)
    # display(df)

    ## Drop samples with negative inc deaths.
    df_deaths = df_deaths[df_deaths.True_inc_deaths >= 0]
    # display(df_deaths)

    # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # 

    from epsampling.utils import drop_duplicate_cols

    # timestamp = '20241009-143022' 

    fpath = os.path.join(DATA_DIR,'processed',f'formatted_acs_results_normed_{timestamp}.csv')
    df_acs = pd.read_csv(fpath,index_col='Fips')

    # Remove duplicate columns
    df_acs = drop_duplicate_cols(df_acs)

    ## Standardize
    df_acs=(df_acs-df_acs.mean())/df_acs.std()

    df = df_deaths.merge(df_acs, on='Fips')

    return df, df_acs


# from epsampling.utils import get_chunks

# # def get_date_chunked_splits(df, chunks, chunk_idx, feat_cols, target_col='Target', naive_col='Naive'):
# def get_date_chunked_splits(df, chunks, chunk_idx):

#     dates_test = chunks[chunk_idx]
#     dates_train = [x for x in df.Date.unique() if x not in dates_test]

#     df_train = df[df.Date.isin(dates_train)]
#     df_test = df[df.Date.isin(dates_test)]

# #     X_train = df_train[feat_cols]
# #     X_test = df_test[feat_cols]

# #     y_train = df_train[target_col]
# #     y_test = df_test[target_col]
    
# #     if naive_col is not None:

# #         y_naive = df_test[naive_col]
    
#     return df_train, df_test


from epsampling.utils import get_chunks

# def get_date_chunked_splits(df, chunks, chunk_idx, feat_cols, target_col='Target', naive_col='Naive'):
def get_date_chunked_splits(df, chunks, chunk_idx, samp_frac=None):

#     samp_frac = 0.5
#     chunk_idx = 18
#     chunks = get_chunks(list(df.Date.unique()), 4)

    dates_test = chunks[chunk_idx]
    dates_train = [x for x in df.Date.unique() if x not in dates_test]

    df_train = df[df.Date.isin(dates_train)]
    df_test = df[df.Date.isin(dates_test)]

    if samp_frac is not None:
        df_train = df_train.sample(frac=samp_frac)
        df_test = df_test.sample(frac=samp_frac)
        
    return df_train, df_test




from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, median_absolute_error
# from sktime.performance_metrics.forecasting import mean_relative_absolute_error
# from sktime.performance_metrics.forecasting import median_relative_absolute_error

def get_performance(model_names, model_preds, y_test):
    
    metrics_dict = {'MAE': mean_absolute_error,
                    'MedAE': median_absolute_error,
                    'MSE': mean_squared_error,
                    'RMSE': mean_squared_error,
                    'r2': r2_score,
#                     'relMAE': mean_relative_absolute_error,
#                     'relMedAE': median_relative_absolute_error
                   }

    model_res_dict = {model:{} for model in model_names}
    
    for model,pred in zip(model_names, model_preds):
        for metric, func in metrics_dict.items():
#             if metric=='relMAE':
#                 model_res_dict[model][metric] = func(y_test, pred, y_pred_benchmark=y_naive)
#             elif metric=='relMedAE':
#                 model_res_dict[model][metric] = func(y_test, pred, y_pred_benchmark=y_naive)
            if metric=='RMSE':
                model_res_dict[model][metric] = func(y_test, pred, squared=False)
            else:
                model_res_dict[model][metric] = func(y_test, pred)
            
    return model_res_dict



def get_metrics_ser(df, target_col, pred_col, 
                    naive_true_col='Naive_true_deaths_x', naive_proj_col='Naive_proj_deaths_x'):
    
    metrics_dict = {'MAE': mean_absolute_error,
                    'MedAE': median_absolute_error,
                    'MSE': mean_squared_error,
                    'RMSE': mean_squared_error,
                    'r2': r2_score
                    }
    
    mae = mean_absolute_error(df[target_col], df[pred_col])
    medae = median_absolute_error(df[target_col], df[pred_col])
    r2 = r2_score(df[target_col], df[pred_col])
    mse = mean_squared_error(df[target_col], df[pred_col])
    
    # relMAE to naive_true
    relmae_true = mae / mean_absolute_error(df[target_col],df[naive_true_col])
    # relMAE to naive_proj
    relmae_proj = mae / mean_absolute_error(df[target_col],df[naive_proj_col])
    
    ser = {'Model':pred_col, 'MAE':mae, 'MedAE':medae, 
           'R-squared':r2, 'MSE': mse, 'relMAE (Proj)': relmae_proj, 
           'relMAE (True)': relmae_true}
    
    return ser
    
