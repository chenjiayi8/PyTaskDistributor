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
setup['hostname'] = 'cmmb01'
obj = Server(setup, debug=True)
obj.statusDict['CPU_total'] = 50.0
obj.statusDict['MEM_total'] = 50.0
obj.prepareFactory()
taskList = obj.getTaskList()
task = taskList[0]
obj.updateFolderPaths(task)#paths for output
df = obj.getTaskTable(task)# chec
df = obj.removeFinishedInputs(df)
sessions = obj.createSessions(df)
sessions = obj.workloadBalance(sessions)
obj.runSessions(sessions)
keys = list(obj.sessionsDict.keys())
session = obj.sessionsDict[keys[0]]
for i in range(10):
    time.sleep(30)
    obj.updateSessionsStatus()
#obj.removeFinishedTask()
