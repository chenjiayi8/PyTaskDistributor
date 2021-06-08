#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:16:55 2021

@author: frank
"""

import json
import pandas as pd
from collections import OrderedDict
from json import JSONDecoder
customdecoder = JSONDecoder(object_pairs_hook=OrderedDict)

def writeJSON_from_dict(path, dict_obj):
    with open(path, "w+") as outfile: 
        json.dump(dict_obj, outfile, indent = 4)
        
        
def writeJSON_from_df(path, df):
    out_dict = (df.reset_index(drop=True)
       .fillna('')
       .to_dict('list'))
    ordered_dict = OrderedDict((k,out_dict.get(k)) for k in df.columns)
    writeJSON_from_dict(path, ordered_dict)
        
def readJSON_to_df(path):
    with open(path, 'r') as f:
        json_str = f.readlines()
        json_str = '\n'.join(json_str)
        json_dict = customdecoder.decode(json_str)
        df = pd.DataFrame.from_dict(json_dict)
        df = df[list(json_dict.keys())]
        return df

def readJSON_to_dict(path):
    with open(path, 'r') as f:
        json_str = f.readlines()
        json_str = '\n'.join(json_str)
        json_dict = customdecoder.decode(json_str)
#        df = pd.read_json(json_str)
#        df = df[list(json_dict.keys())]
        return json_dict
