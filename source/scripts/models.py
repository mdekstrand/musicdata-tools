"""
Use a model to produce recommendations

Usage:
    generaterecs.py [options]

Options:
    -v, --verbose         Enable detailed (DEBUG-level) logging
    --log-file FILE       Write logs to FILE
"""

import pandas as pd
import numpy as nop
import pyarrow.parquet as pq
import duckdb 
import statsmodels.api as sm
import statsmodels.tsa.arima.model as smt
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from lenskit import als
from lenskit.als import BiasedMFScorer
from lenskit.batch import recommend
from lenskit.data import ItemListCollection, UserIDKey, load_movielens, from_interactions_df
from lenskit.knn import ItemKNNScorer
from lenskit.metrics import NDCG, RBP, RecipRank, RunAnalysis
from lenskit.pipeline import topn_pipeline
from lenskit.splitting import SampleFrac, crossfold_users
from lenskit.data.matrix import MatrixDataset
from lenskit.basic.popularity import PopScorer
from lenskit import util
import logging
from datetime import *
from dateutil.relativedelta import *
import os
from docopt import docopt
from lenskit.logging.config import LoggingConfig
from lenskit.parallel import invoker, get_parallel_config

_log = logging.getLogger("run-model")

# Parameters
step_size = 2
n=100 #rec lists length
window_size = 24
algorithm_name = 'mostpop'

# Define the initial start date and data period
start_date = pd.to_datetime('2007-01-01')
data_from = '2007-01-01'
data_to = '2017-10-31'

def generate_time_windows(actions, start_date, step_size, window_size):
    end_date = start_date + relativedelta(months=window_size)
    
    time_windows = []
      
    while end_date <= actions.index.max():

        _log.info("extracting time windows")
        time_windows.append((start_date, end_date))

        start_date += relativedelta(months=step_size)
        end_date += relativedelta(months=step_size)

    return time_windows  

def worker_function(data, time_period):

    rec_directory = f'../recs/{algorithm_name}'
    os.makedirs(rec_directory, exist_ok=True)
    
    # model = als.ImplicitMF(50, use_ratings=False)
    model = PopScorer()

    start_date, end_date = time_period

    _log.info("preparing window data")
    window_data = data.loc[start_date:end_date-relativedelta(days=1)]
       
    if len(window_data) < 10:
        return None
        
    _log.info("preparing test and train")
    max_date = window_data.index[-1]
    test_start = max_date - relativedelta(months=step_size)
    
    train_data = window_data.loc[:test_start-relativedelta(days=1)].reset_index(drop=True)
    test_data = window_data.loc[test_start:].reset_index(drop=True)
    
    # Ensure test users exist in the training data
    _log.info("Excluding unseen users from test set")
    train_users = set(train_data['user_id'])
    test_data = test_data[test_data['user_id'].isin(train_users)]
    
    train_size = train_data.shape[0]
    test_size = test_data.shape[0]
    
    if len(test_data) < 2:
        return None
        
    train_dic = from_interactions_df(train_data)
    als_test = ItemListCollection.from_df(test_data,['user_id'])
    
    _log.info("training the model")
    fit_als = topn_pipeline(model)
    fit_als.train(train_dic)
        
    _log.info("generating recommendations")
    als_recs = recommend(fit_als, als_test.keys(), n)

    _log.info("Saving recommendations to parquet file")
    file_path = os.path.join(rec_directory, f'{start_date.date()}.parquet')
    als_recs.save_parquet(file_path)
    
    _log.info("running analysis")
    ran = RunAnalysis()
    ran.add_metric(NDCG())
    ran.add_metric(RBP())
    ran.add_metric(RecipRank())
    results = ran.compute(als_recs, als_test)
    
    metrics_list = {
        'start_date': start_date, 
        'end_date': end_date, 
        'ndcg': results.list_metrics().mean().NDCG,
        'rbp': results.list_metrics().mean().RBP,
        'reciprank': results.list_metrics().mean().RecipRank
    }
    
    metrics_list = pd.DataFrame([metrics_list])
    
    return metrics_list, train_size, test_size

    
def process_time_windows(data, time_windows):
       
    with invoker(data, worker_function) as invk: 
        results = list(invk.map(time_windows))  
    
    metrics_results, train_sizes, test_sizes = zip(*results)

    metrics_list = pd.concat(metrics_results, ignore_index=True)
    train_sizes = list(train_sizes)
    test_sizes = list(test_sizes)
    
    return metrics_list, train_sizes, test_sizes


if __name__ == '__main__':

    args = docopt(__doc__)
    lcfg = LoggingConfig()

    if args["--verbose"]:
        lcfg.set_verbose()  
    if args["--log-file"]:
        lcfg.log_file(args["--log-file"], logging.DEBUG) 

    lcfg.apply()

    # define constants
    data_path = '../../data/'

    work_ratings = data_path + 'gr-work-ratings.parquet'
    work_info = data_path + 'gr-work-info.parquet'
    work_gender = data_path + 'gr-work-gender.parquet'
    work_actions = data_path + 'gr-work-actions.parquet'

    # And a reference point - the recommender was added on September 15, 2011:
    date_rec_added = pd.to_datetime('2011-09-15')
    date_rec_added - pd.to_timedelta('1W')

    # load data
    actions = pd.read_parquet(work_actions, columns=['user', 'item','first_time'])
    actions.rename(columns={'first_time': 'timestamp','user':'user_id','item':'item_id'}, inplace=True)
    
    # convert 'first_time' column to readable timestamp
    actions['timestamp'] = pd.to_datetime(actions['timestamp'], unit='s')
    
    actions.sort_values('timestamp', inplace=True)
    actions.set_index('timestamp', inplace=True)
    
    
    # select a set of 200 random users
    # initial_users = np.random.choice(data['user'].unique(), 200, replace=False)
    # data = data[data['user'].isin(initial_users)] 
    
    
    # Truncate the data to only include full months, and include 2006 since there was little activity
    actions = actions.loc[data_from:data_to]
    actions['rating'] = 1

    # exlude users with less than 5 ratings
    actions = actions[actions['user_id'].map(actions['user_id'].value_counts()) >= 5]

    time_windows = generate_time_windows(actions, start_date, step_size, window_size)
    
    # Process time windows in parallel
    results = process_time_windows(actions, time_windows)
    i = results[0].shape[0]
    metrics_list = results[0]
    metrics_list.describe()
    
    # check if plots directory exists
    plot_path = f'../plots/{algorithm_name}/'
    os.makedirs(plot_path, exist_ok=True)

    #save metrics to a csv file
    metrics_list.to_csv(f'{plot_path}{step_size}step_metrics.csv', index=False)

    # plot test and train data sizes
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    
    ax1.plot(np.arange(i), results[1], label='Train Size', color='b')
    ax1.scatter(np.arange(i), results[1], color='b', s=10)  
    ax1.set_title('Train Data Sizes')
    ax1.set_xlabel('Iteration')
    ax1.set_ylabel('Size')
    ax1.grid(True)
    
    ax2.plot(np.arange(i), results[2], label='Test Size', color='r')
    ax2.scatter(np.arange(i), results[2], color='r', s=10)  
    ax2.set_title('Test Data Sizes')
    ax2.set_xlabel('Iteration')
    ax2.set_ylabel('Size')
    ax2.grid(True)
    
    plt.tight_layout()
    fig.savefig(f'{plot_path}{step_size}step_datasize.jpg', dpi=300)
    plt.close(fig)
    
    
    #  plot metrics
    fig, ((ax1, ax2), (ax3, _)) = plt.subplots(2, 2, figsize=(14, 12)) 
    
    ax1.plot(metrics_list['start_date'], metrics_list['ndcg'], color='c')  
    ax1.set_title("Recommender Model NDCG@100 Over Time")
    ax1.set_xlabel("Time Window Start Date")
    ax1.set_ylabel("NDCG")
    ax1.tick_params(axis='x', rotation=45)
    
    ax2.plot(metrics_list['start_date'], metrics_list['rbp'], color='b') 
    ax2.set_title("Recommender Model RBP@100 Over Time")
    ax2.set_xlabel("Time Window Start Date")
    ax2.set_ylabel("RBP")
    ax2.tick_params(axis='x', rotation=45)
    
    ax3.plot(metrics_list['start_date'], metrics_list['reciprank'], color='r') 
    ax3.set_title("Recommender Model RecipRank@100 Over Time")
    ax3.set_xlabel("Time Window Start Date")
    ax3.set_ylabel("RecipRank")
    ax3.tick_params(axis='x', rotation=45)
    _.axis('off')
    
    plt.tight_layout()  
    fig.savefig(f'{plot_path}{step_size}step_measures.jpg', dpi=300)
    plt.close(fig)
