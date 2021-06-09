#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

import os
from PyTaskDistributor.util.config import readConfig, getHostName
from PyTaskDistributor.core.master import Master
import random

config_path= os.path.join(os.getcwd(), 'config.txt')

config = readConfig(config_path)
hostname = getHostName()

#hostname = 'cmmb01'
setup = config[hostname]
#setup['order'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
#setup['delivery'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
#setup['factory'] = '/home/frank/LocalWorkFolder/TranReNu/MC3DVersion3.3_Git/'        
obj = Master(setup)
#obj.main()
#obj.generateTasks()
#obj.updateServerList()
#obj.updateTaskStatus()
#
#cmd_tempate = "nohup matlab-R2016B -r 'D3Q7Initiator({});exit;' > {}"
#inputs = ['1.0,86400.0,40.0,40.0,0.4,1000000.0,9.0,5.0,8.0,40.0,40.0,40.0,40.0,0.0',
#          'Task-1.txt']



