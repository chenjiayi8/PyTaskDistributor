#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

import os
import time
from PyTaskDistributor.util.config import readConfig, getHostName
from PyTaskDistributor.core.server import *

config_path= os.path.join(os.getcwd(), 'config.txt')
config = readConfig(config_path)
hostname = getHostName()
setup = config[hostname]     
obj = Server(setup)
#obj.removeFinishedTask()
