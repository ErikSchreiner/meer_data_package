import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.dates import DateFormatter
import numpy as np
import os
# https://arrow.apache.org/docs/python/parquet.html
import pyarrow as pa
import pyarrow.parquet as pq
from icecream import ic


# set matplotlib graphs settings
def matplotlib_graph_settings():
    font=17
    line=2
    plt.rcParams['xtick.bottom'] = True
    plt.rcParams['ytick.left'] = True
    plt.rcParams['xtick.direction'] = 'in'
    plt.rcParams['ytick.direction'] = 'in'
    plt.rcParams['mathtext.default'] = 'regular'
    # Change font size: http://www.futurile.net/2016/02/27/matplotlib-beautiful-plots-with-style/
    plt.rcParams['font.size'] = font
    plt.rcParams['axes.labelsize'] = font
    plt.rcParams['axes.labelweight'] = 'regular'
    plt.rcParams['axes.titleweight'] = 'regular'
    plt.rcParams['figure.labelweight'] = 'regular'
    plt.rcParams['xtick.labelsize'] = font
    plt.rcParams['ytick.labelsize'] = font
    plt.rcParams['legend.fontsize'] = font
    plt.rcParams['figure.titlesize'] = font
    plt.rcParams['lines.linewidth'] = line
    plt.rcParams['axes.linewidth'] = line
    plt.rcParams['lines.markersize'] = 9


    plt.rcParams['xtick.direction'] = 'out'
    plt.rcParams['xtick.minor.visible'] = True
    plt.rcParams['ytick.direction'] = 'out'
    plt.rcParams['ytick.minor.visible'] = True
    plt.rcParams["xtick.major.width"] = line
    plt.rcParams["xtick.minor.width"]= line
    plt.rcParams["xtick.major.size"] = 6.5
    plt.rcParams["xtick.minor.size"] = 3.4
    plt.rcParams["ytick.major.width"] = line
    plt.rcParams["ytick.minor.width"]= line
    plt.rcParams["ytick.major.size"] = 6.5
    plt.rcParams["ytick.minor.size"] = 3.4


# returns a timezone aware datetime object. Will also convert timezone.
def tz_aware_dt(datetime_str, datetime_fmt='%Y-%m-%d %H:%M:%S', localize="US/Pacific", convert="US/Pacific"):
    if datetime_fmt[-2:] == "%z":
        tmp = datetime_str.tz_convert(convert) # convert timezone
        return tmp
    if datetime_str == "current":
        print("Returned timestamp localized to: ", localize)
        return pd.Timestamp.today().floor('S').tz_localize(localize)
    else:
        tmp = pd.to_datetime(datetime_str, format=datetime_fmt)
        tmp = tmp.tz_localize(localize, ambiguous='NaT') # make timezone aware (Eastern)
        if localize != convert:
            tmp = tmp.tz_convert(convert) # convert timezone (to Pacific)
        return tmp
