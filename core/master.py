#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:05:04 2020

@author: frank
"""

from PyTaskDistributor.util.json import (
        readJSON_to_df, writeJSON_from_df, readJSON_to_dict)
from PyTaskDistributor.util.others import updateXlsxFile, sleepMins
from PyTaskDistributor.util.monitor import Monitor
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
#import time
import shutil

def getUUID(origin):
    rd = random.Random()
#    rd.seed(0)
    uuid_str = str(uuid.UUID(int=rd.getrandbits(128)))
    return uuid_str[:5]

class Master:
    def __init__(self, setup, fastMode=False):
        self.setup = setup
        self.defaultFolder = os.getcwd()
        self.fastMode = fastMode
        self.serverFolder = os.path.join(self.defaultFolder, 'Servers')
        self.mainFolder = self.setup['order']
        self.newTaskFolder = os.path.join(self.mainFolder, 'NewTasks')
        self.finishedTaskFolder = os.path.join(self.mainFolder,
                                               'FinishedTasks')
        self.taskFilePath = os.path.join(self.mainFolder, 'TaskList.xlsx')
        self.lastModifiedTime = ''
        self.monitor = Monitor(self)
        self.msgs = []
        pass
    
    def main(self):
        numMin = random.randint(3,5)
        try:
            if self.lastModifiedTime != self.getFileLastModifiedTime():
                self.generateTasks()
            self.updateServerList()
            self.updateTaskStatus()
            self.removeFinishedTask()
            self.workloadBalance()
            self.printProgress(numMin)
            self.printMsgs()
            sleepMins(numMin)
            needAssistance = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print ("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            needAssistance = True
        return needAssistance
     
    def printProgress(self, numMin=5):
        self.monitor.printProgress(numMin)
        
    def printMsgs(self):
        for msg in self.msgs:
            print(msg)
        self.msgs.clear()
    
    def getFileLastModifiedTime(self, taskFilePath=''):
        if len(taskFilePath) == 0:
            taskFilePath = self.taskFilePath
        taskTable_modifiedTime = datetime.fromtimestamp(\
                            os.path.getmtime(taskFilePath))
        return taskTable_modifiedTime
    
    def getTaskList(self, folder=None):
        if folder == None:
            folder = self.newTaskFolder
        taskList = [f for f in os.listdir(folder) if f.endswith(".json")]
        return taskList
    
    def updateServerList(self, timeout_mins=30):
        serverList = [f for f in os.listdir(self.serverFolder)\
                      if f.endswith(".json")]
        self.serverList = []
        for s in serverList:
            try:
                s_path = os.path.join(self.serverFolder, s)
                if 'sync-conflict' in s:
                    os.unlink(s_path)
                else:
                    s_json = readJSON_to_dict(s_path)
                    s_time = dateutil.parser.parse(s_json['updated_time'])
                    diff_min = (datetime.now() - s_time).seconds/60
                    if diff_min < timeout_mins:#only use if updated within N mins
                        self.serverList.append(s_json)
            except:
                print("Failed to read status of {}".format(s))
                pass
                
    def workloadBalance(self):
        taskList = self.getTaskList()
        if len(taskList) == 0:
            return
        for task in taskList:
            if 'sync-conflict' in task:#conflict file from Syncthing
                task_path = os.path.join(self.newTaskFolder, task)
                os.unlink(task_path)
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
            msg_cause = ''
            df_temp = df[df['HostName']=='']
            #No more sessions
            if len(df_temp) == 0:
                skipFlag = True
                msg_cause += 'All sessions are assigned\n'
            #assigned sessions but not running
            if len(server['currentSessions']) > server['num_running']:
                skipFlag = True
                msg_cause += 'Assigned sessions are not running\n'
            df_assigned = df[(df['HostName']==server['name'])&\
                             (df['Finished']!=1)]
            #assigned sessions but not received
            for idx in df_assigned.index:
                if idx not in server['currentSessions']:
                    finishedSessions = server['finishedSessions'].keys()
                    idx_in_finishedSessions = [idx in s for s in finishedSessions]
                    if not any(idx_in_finishedSessions):
                        if not skipFlag:
                            msg_cause += '\n'
                        skipFlag = True
                        msg_cause += 'Assigned session {} are not received\n'.format(idx)
        
            if not skipFlag:
                if int(server['num_running']) == 0:
                    num_target = 1
                    if self.fastMode:
                        num_target = math.ceil(len(df_temp)/len(self.serverList))
                else:
                    cpu_avaiable = server['CPU_max'] - server['CPU_total']
                    cpu_per_task = server['CPU_matlab'] / server['num_running']
                    num_cpu = math.floor(cpu_avaiable/cpu_per_task)
                    mem_avaiable = server['MEM_max'] - server['MEM_total']
                    mem_per_task = server['MEM_matlab'] / server['num_running']
                    num_mem = math.floor(mem_avaiable/mem_per_task)
                    num_target = min([num_cpu, num_mem])
                    if num_target > 2: #Max add 2 per cyce 
                        num_target = 2
                    if num_target < 0:
                        num_target = 0
                # CPU/MEM limit
                if num_target == 0:
                    skipFlag = True
                    msg_cause += 'reaching CPU/MEM limit'
                
            # Do not assign new session
            if skipFlag: 
                msg = "Assign 0 new sessions of {} for Server {} because: {}"\
                        .format(task, server['name'], msg_cause)
            else:# assign new session
                intialTaskIdx = list(df_temp.index)
                random.shuffle(intialTaskIdx)
                if len(intialTaskIdx) > num_target:
                    intialTaskIdx = intialTaskIdx[:num_target]
                else:
                    num_target = len(intialTaskIdx)
                for i in range(len(intialTaskIdx)):
                    df.loc[intialTaskIdx[i], 'HostName'] = server['name']
                msg = "Assign {} new sessions of {} for Server {}"\
                            .format(num_target, task, server['name'])
            # record msg
            self.msgs.append(msg)
        writeJSON_from_df(task_path, df)
    
    def getFinishedSessions(self):
        finishedSessions = {}
        for server in self.serverList:   
            tempDict = server['finishedSessions']
            for _, v in tempDict.items():
                v['HostName'] = server['name']
            finishedSessions.update(tempDict)
        return finishedSessions
    
    def getTimeStr(self, task):
        markLocation = [i for i, ltr in enumerate(task) if ltr == '_']
        dotLocation = [i for i, ltr in enumerate(task) if ltr == '.']
        timeStr = task[markLocation[0]+1:dotLocation[-1]]
        return timeStr
    
    def readTask(self, path):
        df = readJSON_to_df(path)
        df = df.sort_values('Num')
        temp = list(zip(['Task']*len(df),\
                df['Num'].apply(str) , df['UUID']))
        index = ['-'.join(t) for t in temp]
        df.index = index
        df = df.fillna('')
        return df
    
    def markTaskDF(self, task, df, finishedSessions):
        modified_flag = False
        for key in finishedSessions.keys():
            underlineLocs = [i for i, ltr in enumerate(key) if ltr == '_']
            taskTimeStr = key[:underlineLocs[1]]
            index = key[underlineLocs[1]+1:]
            if taskTimeStr in task and index in df.index:
                #only modify when necessary
                if df.loc[index, 'Finished'] != 1:
                    values = finishedSessions[key]
                    modified_flag = True
                    for k, v in values.items():
                        df.loc[index, k] = v
        return modified_flag
    
    def removeFinishedTask(self):
        taskList = self.getTaskList(folder=self.finishedTaskFolder)
        if len(taskList) == 0:#skip if no new finished task
            return
        finishedSessions = self.getFinishedSessions()
        for task in taskList:
            path = os.path.join(self.finishedTaskFolder, task)
            path_xlsx = os.path.join(self.mainFolder,\
                                     'Output', task[:-5]+'.xlsx')
            df = self.readTask(path)
            modified_flag = self.markTaskDF(task, df, finishedSessions)
            if modified_flag:
                writeJSON_from_df(path, df)
                updateXlsxFile(path_xlsx, df)
            if all(df['Finished'] == 1):# rename to json.bak 
                path_done = os.path.join(self.finishedTaskFolder, task+'.bak')
                shutil.move(path, path_done)
        
    def updateTaskStatus(self):
        taskList = self.getTaskList()
        finishedSessions = self.getFinishedSessions()
        for task in taskList:
            path = os.path.join(self.newTaskFolder, task)
            path_xlsx = os.path.join(self.mainFolder,\
                                     'Output', task[:-5]+'.xlsx')
            df = self.readTask(path)
            modified_flag = self.markTaskDF(task, df, finishedSessions)
            if modified_flag:
                writeJSON_from_df(path, df)
                updateXlsxFile(path_xlsx, df)
            if all(df['Finished'] == 1):# move to finish folder 
                path_done = os.path.join(self.finishedTaskFolder, task)
                shutil.move(path, path_done)
    
    def generateManualTasks(self):
        manualXlsx = os.path.join(self.mainFolder, 'TaskList_manual.xlsx')
        if os.path.isfile(manualXlsx):
            lastModifiedTime = self.getFileLastModifiedTime(manualXlsx)
            lastModifiedTimeStr = datetime.strftime(\
                                    lastModifiedTime, "%Y%m%d_%H%M%S")
            oldJsonName = os.path.join(self.finishedTaskFolder,
                                   'TaskList_'+lastModifiedTimeStr+'.json')
            newJsonName = os.path.join(self.newTaskFolder,
                                   'TaskList_'+lastModifiedTimeStr+'.json')
            if os.path.isfile(oldJsonName):# already finished
                return
            if os.path.isfile(newJsonName):# already created
                return
            df = pd.read_excel(manualXlsx)
            df = df.fillna('')
            df.loc[:, 'UUID'] = df.loc[:, 'UUID'].apply(getUUID)
            writeJSON_from_df(newJsonName, df)
    
    def generateTasks(self):
        self.lastModifiedTime = self.getFileLastModifiedTime()
        lastModifiedTimeStr = datetime.strftime(\
                                self.lastModifiedTime, "%Y%m%d_%H%M%S")
        oldJsonName = os.path.join(self.finishedTaskFolder,
                               'TaskList_'+lastModifiedTimeStr+'.json')
        newJsonName = os.path.join(self.newTaskFolder,
                               'TaskList_'+lastModifiedTimeStr+'.json')
        if os.path.isfile(oldJsonName):# already finished
            return
        if os.path.isfile(newJsonName):# already created
            return
        modifiedTime = os.path.getmtime(self.taskFilePath)
        parameterTable = pd.read_excel(self.taskFilePath,
                                       sheet_name='ParameterRange')
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
            vector = [float(vector[i]) for i in range(len(vector))
                            if nonNanIdx[i]]
            vectorList.append(vector)
        
        combinations = list(prod(*vectorList))
        combinations = [[i+1]+list(combinations[i]) 
                            for i in range(len(combinations))]
        taskTable = pd.DataFrame(data=combinations, columns=columns)
        taskTable_old = pd.read_excel(self.taskFilePath, sheet_name='Sheet1')
        columns_old = list(taskTable_old.columns)
        for c in columns_old:
            if c not in taskTable.columns:
                taskTable[c] = ''
        taskTable.loc[:, 'UUID'] = taskTable.loc[:, 'UUID'].apply(getUUID)
        updateXlsxFile(self.taskFilePath, taskTable)
        taskTable = pd.read_excel(self.taskFilePath, sheet_name = 'Sheet1')
        newXlsxName = os.path.join(self.mainFolder,\
                         'Output', 'TaskList_'+lastModifiedTimeStr+'.xlsx')
        writeJSON_from_df(newJsonName, taskTable)
        shutil.copyfile(self.taskFilePath, newXlsxName)
        os.utime(self.taskFilePath, (modifiedTime, modifiedTime))
        os.utime(newJsonName, (modifiedTime, modifiedTime))
        os.utime(newXlsxName, (modifiedTime, modifiedTime))


if __name__ == '__main__':
    pass
    
