#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:05:04 2020

@author: frank
"""

from PyTaskDistributor.util.json import (
        readJSON_to_df, writeJSON_from_df, readJSON_to_dict)
from PyTaskDistributor.util.others import updateXlsxFile, sleepMins
import os
import sys
from datetime import datetime
import dateutil
import pandas as pd
import numpy as np
from itertools import product as prod
import math
import random
import uuid
import traceback
import time
import shutil

def getUUID(origin):
    rd = random.Random()
#    rd.seed(0)
    uuid_str = str(uuid.UUID(int=rd.getrandbits(128)))
    return uuid_str[:5]

class Master:
    def __init__(self, setup):
        self.setup = setup
        self.defaultFolder = os.getcwd()
        self.serverFolder = os.path.join(self.defaultFolder, 'Servers')
        self.mainFolder = self.setup['order']
        self.newTaskFolder = os.path.join(self.mainFolder, 'NewTasks')
        self.finishedTaskFolder = os.path.join(self.mainFolder, 'FinishedTasks')
        self.taskFilePath = os.path.join(self.mainFolder, 'TaskList.xlsx')
        self.lastModifiedTime = ''
        pass
    
    def main(self):
        numMin = random.randint(3,5)
        try:
            if self.lastModifiedTime != self.getFileLastModifiedTime():
                self.generateTasks()
            self.updateServerList()
            self.updateTaskStatus()
            self.workloadBalance()
            lastModifiedTimeStr = datetime.strftime(self.lastModifiedTime, 
                                                      "%H:%M:%S %d/%m/%Y")
            nowTimeStr = datetime.strftime(datetime.now(),  "%H:%M:%S %d/%m/%Y")
            msg = "{}: Last task is assigned at {}, sleeping for {} mins".format(nowTimeStr, lastModifiedTimeStr, numMin)
            print(msg)
#            print("\r", msg, end='')
            sleepMins(numMin)
            needAssistance = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            needAssistance = True
        return needAssistance
            
    def getFileLastModifiedTime(self):
        taskTable_modifiedTime = datetime.fromtimestamp(os.path.getmtime(self.taskFilePath))
        return taskTable_modifiedTime
    
    def getTaskList(self, folder=None):
        if folder == None:
            folder = self.newTaskFolder
        taskList = [file for file in os.listdir(folder) if file.endswith(".json")]
        return taskList
    
    def updateServerList(self, timeout_mins=30):
        serverList = [file for file in os.listdir(self.serverFolder) if file.endswith(".json")]
        self.serverList = []
        for s in serverList:
            s_path = os.path.join(self.serverFolder, s)
            s_json = readJSON_to_dict(s_path)
            s_time = dateutil.parser.parse(s_json['updated_time'])
            diff_min = (datetime.now() - s_time).seconds/60
            if diff_min < timeout_mins:#only use if updated within N mins
                self.serverList.append(s_json)
                
    def workloadBalance(self):
        taskList = self.getTaskList()
        if len(taskList) == 0:
            return
        task = random.choice(taskList)#Only balance one task per time
        task_path = os.path.join(self.newTaskFolder, task)
        df = readJSON_to_df(task_path)
        df = df.sort_values('Num')
        temp = list(zip(['Task']*len(df), df['Num'].apply(str) , df['UUID']))
        index = ['-'.join(t) for t in temp]
        df.index = index
        df = df.fillna('')
        for server in self.serverList:
            # check if assigned sessions are running
            skipFlag = False
            num_target = 0
            df_assigned = df[(df['HostName']==server['name'])&\
                             (df['Finished']!=1) ]
            for idx in df_assigned.index:
                if idx not in server['currentSessions']:
                    #assigned session is not running
                    skipFlag = True
                    continue
            if skipFlag:
                continue# Do not assign new session
            if int(server['num_matlab']) == 0:
                num_target = 1
            else:
                cpu_avaiable = float(server['CPU_max']) - float(server['CPU_total'])
                cpu_per_task = float(server['CPU_matlab']) / float(server['num_matlab'])
                num_cpu = math.floor(cpu_avaiable/cpu_per_task)
                mem_avaiable = float(server['MEM_max']) - float(server['MEM_total'])
                mem_per_task = float(server['MEM_matlab']) / float(server['num_matlab'])
                num_mem = math.floor(mem_avaiable/mem_per_task)
                num_target = min([num_cpu, num_mem])
                if num_target > 2: #Max add 2 per cyce 
                    num_target = 2
                if num_target < 0:
                    num_target = 0
            df_temp = df[df['HostName']=='']
            if len(df_temp) > 0:#still have some tasks to do
                intialTaskIdx = list(df_temp.index)
                random.shuffle(intialTaskIdx)
                if len(intialTaskIdx) > num_target:
                    intialTaskIdx = intialTaskIdx[:num_target]
                else:
                    num_target = len(intialTaskIdx)
                for i in range(len(intialTaskIdx)):
                    df.loc[intialTaskIdx[i], 'HostName'] = server['name']
            print("Assign {} sessions for Server {}".format(num_target, server['name']))
        writeJSON_from_df(task_path, df)
    
    def getFinishedSessions(self):
        finishedSessions = {}
        for server in self.serverList:   
            tempDict = server['finishedSessions']
            for _, v in tempDict.items():
                v['HostName'] = server['name']
            finishedSessions.update(tempDict)
        return finishedSessions
    
    def updateTaskStatus(self):
        taskList = self.getTaskList()
        finishedSessions = self.getFinishedSessions()
        for task in taskList:
            path = os.path.join(self.newTaskFolder, task)
            path_xlsx = os.path.join(self.mainFolder, 'Output', task[:-5]+'.xlsx')
            df = readJSON_to_df(path)
            df = df.sort_values('Num')
            temp = list(zip(['Task']*len(df), df['Num'].apply(str) , df['UUID']))
            index = ['-'.join(t) for t in temp]
            df.index = index
            df = df.fillna('')
            modified_flag = False
            for key in finishedSessions.keys():
                underlineLocs = [i for i, ltr in enumerate(key) if ltr == '_']
                taskTimeStr = key[:underlineLocs[1]]
                index = key[underlineLocs[1]+1:]
                if taskTimeStr in task and index in df.index:
                    if df.loc[index, 'Finished'] != 1:#only modify when necessary
                        values = finishedSessions[key]
                        modified_flag = True
                        for k, v in values.items():
                            df.loc[index, k] = v
            if modified_flag:
                writeJSON_from_df(path, df)
                updateXlsxFile(path_xlsx, df)
            if all(df['Finished'] == 1):# move to finish folder 
                path_done = os.path.join(self.finishedTaskFolder, task)
                shutil.move(path, path_done)
    
    def generateTasks(self):
        self.lastModifiedTime = self.getFileLastModifiedTime()
        lastModifiedTimeStr = datetime.strftime(self.lastModifiedTime, "%Y%m%d_%H%M%S")
        oldJsonName = os.path.join(self.finishedTaskFolder, 'TaskList_'+lastModifiedTimeStr+'.json')
        newJsonName = os.path.join(self.newTaskFolder, 'TaskList_'+lastModifiedTimeStr+'.json')
        if os.path.isfile(oldJsonName):# already finished
            return
        if os.path.isfile(newJsonName):# already created
            return
        modifiedTime = os.path.getmtime(self.taskFilePath)
        parameterTable = pd.read_excel(self.taskFilePath, sheet_name='ParameterRange')
        parameterIdxs = list(range(0,5))+list(range(7,15+5))
        parameterNames = list(parameterTable.columns)
        parameterNames_Target = [parameterNames[i] for i in parameterIdxs]
        numTotalTask = 1;
        for name in parameterNames_Target:
            vector = parameterTable.loc[:, name]
            numNonNan = list(np.isnan(vector)).count(False)
            numTotalTask *= numNonNan
        
        columns = ['Num'] + parameterNames_Target
        vectorList = []
        for name in parameterNames_Target:
            vector = parameterTable.loc[:, name]
            nonNanIdx = np.isnan(vector)
            nonNanIdx = [not(i) for i in nonNanIdx]
            vector = [float(vector[i]) for i in range(len(vector)) if nonNanIdx[i]]
            vectorList.append(vector)
        
        combinations = list(prod(*vectorList))
        combinations = [[i+1]+list(combinations[i]) for i in range(len(combinations))]
        taskTable = pd.DataFrame(data=combinations, columns=columns)
        taskTable_old = pd.read_excel(self.taskFilePath, sheet_name = 'Sheet1')
        columns_old = list(taskTable_old.columns)
        for c in columns_old:
            if c not in taskTable.columns:
                taskTable[c] = ''
        taskTable.loc[:, 'UUID'] = taskTable.loc[:, 'UUID'].apply(getUUID)
        updateXlsxFile(self.taskFilePath, taskTable)
        taskTable = pd.read_excel(self.taskFilePath, sheet_name = 'Sheet1')
        newXlsxName = os.path.join(self.mainFolder, 'Output', 'TaskList_'+lastModifiedTimeStr+'.xlsx')
        writeJSON_from_df(newJsonName, taskTable)
        shutil.copyfile(self.taskFilePath, newXlsxName)
        os.utime(self.taskFilePath, (modifiedTime, modifiedTime))
        os.utime(newJsonName, (modifiedTime, modifiedTime))
        os.utime(newXlsxName, (modifiedTime, modifiedTime))


if __name__ == '__main__':
    pass
    
