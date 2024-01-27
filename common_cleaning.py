import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.dates import DateFormatter
import numpy as np
import os
# https://arrow.apache.org/docs/python/parquet.html
import pyarrow as pa
import pyarrow.parquet as pq
from icecream import ic


'''
Finds dataloss of a dataframe by taking the time column and finding the change from each pervious time.
Can set a threshold that counts as what counts at dataloss.
'''
def find_dataloss(df, time_col="Time", threshold="5min"):
    data_loss = [] # stores tuples -> (start of loss, end of loss)

    df.reset_index(drop=True, inplace=True)
    time_diff =  df[time_col].diff() # get difference between times
    gaps = time_diff[time_diff > threshold] # find gaps

    # for each gap take the index and get start and end time of gap, add to list as tuple
    for index in gaps.index:
        loss_start = df[time_col].iat[index-1]
        loss_end = df[time_col].iat[index]
        data_loss.append((loss_start, loss_end))

    return data_loss


'''
Take a dataframe and interpolate the columns given a corresponding spacing (time_delta)
start_time and end_time are not recommended as the code will already ceiling and floor the min and max time respectively
so the data is still contained within the given data.

The code creates a dataframe with the desired interpolation as timestamps, merges the dataframes, then interpolates and returns only the
timestamped of the new dataframe.

If catergorical then specificy fill type.
'''
def interpolate_df(df, columns, time_col="Time", time_delta="1min", dataloss=None, categorical=False, start_time=False, end_time=False):
    min_time = df.loc[:,time_col].iat[0] + pd.Timedelta(60 - df.loc[:,time_col].iat[0].second, unit="seconds") #get first time and round up to nearest minute
    max_time = df.loc[:,time_col].iat[-1] - pd.Timedelta(df.loc[:,time_col].iat[-1].second, unit="seconds") #get last time and round down to nearest minute
    if start_time:
        min_time = start_time
    if end_time:
        max_time = end_time
    resample_index = pd.date_range(start=min_time, end=max_time, freq=time_delta) # create range of datetimes, with provided freq

    # create dataframe to later merge with
    dummy_frame = pd.DataFrame(np.NaN, index=resample_index, columns=["Time"])
    dummy_frame.loc[:,"Time"] = resample_index

    # merge such that all values are kept
    df_to_add_to = pd.merge(df, dummy_frame, on=["Time"], how='outer', sort=True)

    # interpolate values
    df_to_add_to.set_index(time_col, inplace=True)
    df_interpolated = df_to_add_to
    if categorical:
        df_interpolated[columns] = df_to_add_to[columns].interpolate(method=categorical)
    else:
        df_interpolated[columns] = df_to_add_to[columns].interpolate(method='time')
    
    # keep only the values in resample index
    df_interpolated = df_interpolated[df_interpolated.index.isin(resample_index)] 
    duplicate_indices = df_interpolated.index.drop_duplicates(keep='first') # most likely repeditive line of code
    df_interpolated = df_interpolated.loc[duplicate_indices,:]

    # if need to make dataloss np.NaN else linearly interploated which is wrong
    if dataloss != None:
        for tuple in dataloss:
            df_interpolated.loc[tuple[0]:tuple[1], :] = np.NaN
    
    return df_interpolated