#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

from PyTaskDistributor.core.master import *
from PyTaskDistributor.util.config import get_host_name, read_config

config_path = os.path.join(os.getcwd(), 'config.txt')

config = read_config(config_path)
hostname = get_host_name()
setup = config[hostname]
setup['hostname'] = 'cmmb01'
obj = Master(setup)
obj.update_server_list(10000)
