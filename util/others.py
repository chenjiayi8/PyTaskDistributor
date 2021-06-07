#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:41:42 2021

@author: frank
"""

import time
import psutil
import pandas as pd

def sleepMins(numMin): #for KeyboardInterrupt 
    for i in range(numMin*60):
        time.sleep(1)
        
         
        
def markFinishedTasks(taskTable, results=[], taskIdx=[]):
#    numTasks = len(taskTable)
    for i in range(len(taskIdx)):
        idx = taskIdx[i]
        result = results[i]
        for key in result.keys():
            taskTable.loc[idx, key] = result[key]            
    return taskTable


def getProcessList():
    data = []
    columns = ['pid', 'User', 'Name', 'CPU', 'Mem', 'Command']
    for proc in psutil.process_iter():
        try:
            # Get process name & pid from process object.
#            cpu_percent = proc.cpu_percent(interval=0.01)
#            print(cpu_percent)
            data.append([proc.pid, proc.username(), proc.name(),
                     0, proc.memory_percent(), proc.exe()])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return pd.DataFrame(data=data, columns=columns)