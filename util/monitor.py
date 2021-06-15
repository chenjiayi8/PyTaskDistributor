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
    msg = tb.tabulate(table.values, table.columns, tablefmt="grid")
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

def isCPUorMEMorDISK(input):
    return any(x in input for x in ['CPU', 'MEM', 'DISK'])


class Monitor():
    def __init__(self, master):
        self.master = master
        self.columns_server = ['name', 'CPU_max', 'MEM_max', 'CPU_total',
                           'MEM_total', 'DISK_total', 'num_assigned', 
                           'num_running','num_finished', 'CPU_matlab',
                           'MEM_matlab',  'updated_time']
        self.columns_task = ['name', 'num_task', 'num_assigned', 
                             'num_finished','updated_time']
        pass
    
    def printProgress(self, numMins=5):
        clearConsole()
        print("Updated on {} and will fresh in {} mins".\
              format(parseTime(datetime.now()), numMins))
        self.printTaskProgress()
        return self.printServerProgress()

    def printTaskProgress(self):
        print("Task:")
        df_tasks = pd.DataFrame(columns=self.columns_task)
        taskList = self.master.getTaskList()
        assignedSessions = []
        finishedSessions = []
        for server in self.master.serverList:
            assignedSessions += server['currentSessions']
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
            num_assigned = len(getDuplicatedItems(assignedSessions, index1))
            num_finished = len(getDuplicatedItems(finishedSessions, index2))
            updated_time = datetime.fromtimestamp(os.path.getmtime(task_path))
            updated_time_str = parseTime(updated_time)
            data = [task[:-5], num_task, num_assigned,
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
        df = df.fillna(0)
        columns_new = [ a+'(%)' if isCPUorMEMorDISK(a) else a for a in df.columns]
        df.columns = columns_new
        df_target = df.loc[:, columns_new[1:]]
        col_head = [columns_new[0]] + df.loc[:, columns_new[0]].values.tolist()
        numColumns = len(df_target.columns)
        numGroups = 2
        numInterval = round(numColumns/numGroups)
        parts = []
        sep_idxs = list(range(0, numColumns, numInterval))
        for i in range(numGroups):
            if i != numGroups-1:
                target_idxs = list(range(sep_idxs[i], sep_idxs[i+1]))
            else:
                target_idxs = list(range(sep_idxs[-1], numColumns))
            df_temp = df_target.iloc[:, target_idxs] 
            part = [list(df_temp.columns)] + df_temp.values.tolist()
            if len(df_temp.columns) < numInterval:
                for p in part:
                    p.insert(0, '')
            for j in range(len(part)):
                part[j] = [col_head[j]] + part[j]
            parts +=part
        table = tb.tabulate(parts,  tablefmt="grid")
        print(table)

if __name__ == '__main__':
    pass
