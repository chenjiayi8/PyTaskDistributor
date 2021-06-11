#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 00:38:06 2021

@author: frank
"""

from PyTaskDistributor.util.json import readJSON_to_df, readJSON_to_dict
from PyTaskDistributor.util.config import readConfig, getHostName
from datetime import datetime
import dateutil
import tabulate as tb
import os
import random
import pandas as pd
from collections import OrderedDict

def printTable(table):
    msg = tb.tabulate(table.values, table.columns, tablefmt="pipe")
    print(msg)

def parseTime(t):
    if type(t) == str:
        t = dateutil.parser.isoparse(t)
    return datetime.strftime(t,  "%d/%m/%Y %H:%M:%S")

def clearConsole():
    command = 'clear'
    if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
        command = 'cls'
    os.system(command)

def getTimeStr(task):
    markLocation = [i for i, ltr in enumerate(task) if ltr == '_']
    dotLocation = [i for i, ltr in enumerate(task) if ltr == '.']
    timeStr = task[markLocation[0]+1:dotLocation[-1]]
    return timeStr

def getDuplicatedItems(lst1, lst2):
    return set(lst1) & set(lst2)

def isCPUorMEM(input):
    return any(x in input for x in ['CPU', 'MEM'])


class Monitor():
    def __init__(self, master):
        self.master = master
        self.columns_server = ['name', 'CPU_max', 'MEM_max', 'CPU_total', 'MEM_total', 'num_matlab',
                        'CPU_matlab', 'MEM_matlab', 'updated_time']
        self.columns_task = ['name', 'num_task', 'num_running', 'num_finished',
                        'updated_time']
        pass
    
    def updateProgress(self, numMins=5):
        clearConsole()
        print("Updated on {} and will fresh in {} mins".\
              format(parseTime(datetime.now()), numMins))
        self.printTaskProgress()
        self.printServerProgress()

    def printTaskProgress(self):
        print("Task:")
        df_tasks = pd.DataFrame(columns=self.columns_task)
        taskList = self.master.getTaskList()
        runningSessions = []
        finishedSessions = []
        for server in self.master.serverList:
            runningSessions += server['currentSessions']
            finishedSessions += server['finishedSessions'].keys()
        for task in taskList:
            timeStr = getTimeStr(task)
            task_path = os.path.join(self.master.newTaskFolder, task)
            df = readJSON_to_df(task_path)
            df = df.sort_values('Num')
            temp = list(zip(['Task']*len(df), df['Num'].apply(str) , df['UUID']))
            index1 = ['-'.join(t) for t in temp]
            index2 = [timeStr+'_'+'-'.join(t) for t in temp]
            num_task = len(df)
            num_running = len(getDuplicatedItems(runningSessions, index1))
            num_finished = len(getDuplicatedItems(finishedSessions, index2))
            updated_time = datetime.fromtimestamp(os.path.getmtime(task_path))
            updated_time_str = parseTime(updated_time)
            data = [task[:-5], num_task, num_running,
                    num_finished, updated_time_str]
            df_tasks = df_tasks.append(pd.DataFrame(data=[data], columns=self.columns_task))
        printTable(df_tasks)
        
        
        
    def printServerProgress(self):
        print("Server:")
        df = pd.DataFrame(columns=self.columns_server)

        for server in self.master.serverList:
            data = [server[k] for k in self.columns_server]
            df = df.append(pd.DataFrame(data=[data], columns=self.columns_server))
        df['updated_time'] = df['updated_time'].apply(parseTime)
        
        taskList = self.master.getTaskList()
        num_assigned_dict = OrderedDict()
        for task in taskList:
            task_path = os.path.join(self.master.newTaskFolder, task)
            df_temp = readJSON_to_df(task_path)
            
            for server in self.master.serverList:
                num_assigned = len(df_temp[df_temp['HostName'] == server['name']])
                if server['name'] in num_assigned_dict:
                    num_assigned_dict[server['name']] += num_assigned
                else:
                    num_assigned_dict[server['name']] = num_assigned
        idx_num_matlab = list(df.columns).index('num_matlab')
        df.insert(idx_num_matlab, 'num_assigned',
                  list(num_assigned_dict.values()))
        columns_new = [ a+'(%)' if isCPUorMEM(a) else a for a in df.columns]
        columns_new[idx_num_matlab+1] = 'num_running'
        df.columns = columns_new
        
        printTable(df)


if __name__ == '__main__':
    from PyTaskDistributor.core.master import Master
    config_path= os.path.join(os.getcwd(), 'config.txt')
    config = readConfig(config_path)
    hostname = getHostName()
    setup = config[hostname]        
    master = Master(setup)
    master.updateServerList()
    monitor = Monitor(master)
    monitor.updateProgress()
#msg = printTable(df)
#df = pd.DataFrame(columns=['A'])
#df = pd.concat([pd.DataFrame(data=[[i, 1]], columns=['A', 'B']) for i in range(5)],
#          ignore_index=True)


