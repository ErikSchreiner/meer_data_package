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
from a top route scan through and get all parquet paths, load parquet,
and return as dictionary of dataframes with file name as key
'''
def load_parquet(top_root):
    def parquet_paths(top_root):
        roots = {}
        for (root,dirs,files) in os.walk(top_root):
            roots[root] = files
        
        return roots

    def load_pq_table(path):
        tmp = pq.read_table(path)
        tmp = tmp.to_pandas()
        return tmp

    files = parquet_paths(top_root)
    all_data = {}
    for root, file_names in files.items():
        for file_name in file_names:
            file_type = file_name.split(".")[-1]
            name = file_name.split(".")[0]
            if file_type == "parquet":
                tmp = load_pq_table("".join([root,"/",file_name]))
                all_data[name] = tmp
    return all_data