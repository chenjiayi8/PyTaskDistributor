#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 00:38:06 2021

@author: frank
"""

#from PyTaskDistributor.util.json import readJSON_to_df, readJSON_to_dict
from progress.bar import Bar

bar = Bar('Processing', max=20)
for i in range(20):
    # Do some work
    bar.next()
bar.finish()


