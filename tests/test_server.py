#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

from PyTaskDistributor.core.server import *
from PyTaskDistributor.util.config import get_host_name, read_config

config_path = os.path.join(os.getcwd(), 'config.txt')
config = read_config(config_path)
hostname = get_host_name()
setup = config[hostname]
setup['hostname'] = 'cmmb01'
obj = Server(setup, debug=True)
obj.main()
# obj.status_dict['CPU_total'] = 50.0
# obj.status_dict['MEM_total'] = 50.0
# obj.prepareFactory()
# taskList = obj.getTaskList()
# task = taskList[0]
# obj.updateFolderPaths(task)#paths for output
# df = obj.getTaskTable(task)# chec
# df = obj.removeFinishedInputs(df)
# sessions = obj.createSessions(df)
# sessions = obj.workloadBalance(sessions)
# obj.runSessions(sessions)
# keys = list(obj.sessionsDict.keys())
# session = obj.sessionsDict[keys[0]]
# for i in range(10):
#    time.sleep(30)
#    obj.updateSessionsStatus()
# obj.removeFinishedTask()
