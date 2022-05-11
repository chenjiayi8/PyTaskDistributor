#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:16:55 2021

@author: frank
"""

import os
import json
import sys
import traceback
from collections import OrderedDict
from json import JSONDecoder
from datetime import datetime
import pandas as pd

custom_decoder = JSONDecoder(object_pairs_hook=OrderedDict)


def write_json_from_dict(path, dict_obj):
    with open(path, "w") as outfile:
        json.dump(dict_obj, outfile, indent=4)


def write_json_from_df(path, df):
    out_dict = (df.reset_index(drop=True)
                .fillna('')
                .to_dict('list'))
    ordered_dict = OrderedDict((k, out_dict.get(k)) for k in df.columns)
    write_json_from_dict(path, ordered_dict)
    m_time = datetime.fromtimestamp(os.path.getmtime(path))
    now_time = datetime.now()
    os.utime(path, times=(round(now_time.timestamp()), round(m_time.timestamp())))


def read_json_to_df(path):
    try:
        with open(path, 'r') as f:
            json_str = f.readlines()
            json_str = '\n'.join(json_str)
            json_dict = custom_decoder.decode(json_str)
            df = pd.DataFrame.from_dict(json_dict)
            df = df[list(json_dict.keys())]
            return df
    except:
            print("Need assistance for unexpected error:\n {}".format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            raise OSError('Error to read_json_to_df: {}'.format(path))


def read_json_to_dict(path):
    with open(path, 'r') as f:
        json_str = f.readlines()
        json_str = '\n'.join(json_str)
        json_dict = custom_decoder.decode(json_str)
        #        df = pd.read_json(json_str)
        #        df = df[list(json_dict.keys())]
        return json_dict
