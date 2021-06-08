#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:43:44 2021

@author: frank
"""

from PyTaskDistributor.util.json import (
        writeJSON_from_dict, readJSON_to_df, writeJSON_from_df,
        readJSON_to_dict)

if __name__ == '__main__':
    import pandas as pd
    from collections import OrderedDict
    import os
    path = os.path.join(os.getcwd(), 'Temp.json')
    df1 = pd.DataFrame(data=[[1, 2, 3], [4, 5, 6]], columns=['a', 'b', 'c'])
    df1.reset_index(drop=True)
    writeJSON_from_df(path, df1)
    df2 = readJSON_to_df(path)
    df2.reset_index(drop=True)
    assert df1.equals(df2), 'Dfs are identical'
    dict_obj1 = OrderedDict()
    dict_obj1['a'] = [1, 4]
    dict_obj1['b'] = [2, 5]
    dict_obj1['c'] = [3, 6]
    writeJSON_from_dict(path, dict_obj1)
    dict_obj2 = readJSON_to_dict(path)
    assert dict_obj1 == dict_obj2, 'Dicts are identical'
    os.unlink(path)
    print("test_json passed")
    