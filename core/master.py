#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:05:04 2020

@author: frank
"""
import os
import sys
from datetime import datetime
import dateutil
import pandas as pd
import numpy as np
from itertools import product as prod
#from Helper import Helper
from openpyxl import load_workbook
import pickle
import math
import random
import uuid
import traceback
import time
import json

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
        numMin = random.randint(1,5)
        try:
            if self.lastModifiedTime != self.getFileLastModifiedTime():
                self.generateInputs()
            lastModifiedTimeStr = datetime.strftime(self.lastModifiedTime, 
                                                      "%H:%M:%S %d/%m/%Y")
            nowTimeStr = datetime.strftime(datetime.now(),  "%H:%M:%S %d/%m/%Y")
            msg = "{}: Last task is assigned at {}, sleeping for {} mins".format(nowTimeStr, lastModifiedTimeStr, numMin)
#            print(msg)
            print("\r", msg, end='')
            time.sleep(numMin*60)
#            Helper.sleep(numMin)
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
    
    def emptySheetExcludeHeaders(self, ws):
#        for row in ws.columns:
        for i, row in enumerate(ws.iter_rows()):     
            if i != 0:                      
                for cell in row:
                    cell.value = None
    
    def updateServerList(self):
        serverList = [file for file in os.listdir(self.serverFolder) if file.endswith(".json")]
        self.serverList = []
        for s in serverList:
            s_path = os.path.join(self.serverFolder, s)
            with open(s_path, 'r') as f:
                s_json = json.load(f)
                s_time = dateutil.parser.parse(s_json['updated_time'])
                diff_min = (datetime.now() - s_time).seconds/60
                if diff_min < 120:#only use if updated within N mins
                    self.serverList.append(s_json)
                
    def generateInputs(self):
        self.updateServerList()
        self.lastModifiedTime = self.getFileLastModifiedTime()
        lastModifiedTimeStr = datetime.strftime(self.lastModifiedTime, "%Y%m%d_%H%M%S")
        oldFileName = os.path.join(self.finishedTaskFolder, '_'+lastModifiedTimeStr+'.json')
        if os.path.isfile(oldFileName):
            return
        parameterTable = pd.read_excel(self.taskFilePath, sheet_name='ParameterRange')
#        Helper.printTable(parameterTable, 'parameterTable')
        parameterIdxs = list(range(0,5))+list(range(7,15+5))
        parameterNames = list(parameterTable.columns)
        parameterNames_Target = [parameterNames[i] for i in parameterIdxs]
#        ServerNameList = parameterTable.loc[:, 'ServerName']
#        ServerNameList = [name for name in ServerNameList if type(name) == str]
#        ServerFactorList = parameterTable.loc[:, 'ServerFactor']
#        ServerFactorList = [factor for factor in ServerFactorList if not np.isnan(factor)]
#        ServerFactorArray = np.array(ServerFactorList)
#        ServerFactorArray = ServerFactorArray/sum(ServerFactorArray)
#        numServerName = len(ServerNameList)
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
        numTasks = len(taskTable)
#        numTaskPerServer = math.floor(numTasks/numServerName)
#        numLastServer = numTasks - (numServerName-1)*numTaskPerServer
        
#        ServerTasks = [];
#        numAssignedTasks = 0
#        for i in range(numServerName-1):
#            numTaskThisServer = int(round(numTasks*ServerFactorArray[i]))
#            numAssignedTasks += numTaskThisServer
#            ServerTasks += [ServerNameList[i]]*numTaskThisServer
#        
#        if numTasks > numAssignedTasks:
#            ServerTasks += [ServerNameList[-1]]*(numTasks-numAssignedTasks)
#        taskTable['ServerName'] = ServerTasks
        taskTable_old = pd.read_excel(self.taskFilePath, sheet_name = 'Sheet1')
        columns_old = list(taskTable_old.columns)
        areSameTasks = True
        for name in columns:
            if name != 'Num':
                vector_old = taskTable_old.loc[:, name]
                vector_old = sorted(set(vector_old))
                vector_new = parameterTable.loc[:, name]
                vector_new = sorted(vector_new)
                nonNanIdx = np.isnan(vector_new)
                nonNanIdx = [not(i) for i in nonNanIdx]
                vector_new = [vector_new[i] for i in range(len(vector_new)) if nonNanIdx[i]]
                if vector_new != vector_old:
                    areSameTasks = False
                    print(vector_new)
                    print(vector_old)
        
        taskTable['UUID'] = ''
        taskTable.loc[:, 'UUID'] = taskTable.loc[:, 'UUID'].apply(getUUID)
        book = load_workbook(self.taskFilePath)
        writer = pd.ExcelWriter(self.taskFilePath, engine='openpyxl')
        writer.book = book
        writer.sheets = {ws.title: ws for ws in book.worksheets}
        
        if not areSameTasks:
            taskTable_new = pd.DataFrame(columns=columns_old)
            taskTable_new = pd.concat([taskTable, taskTable_new], axis=1)
#            for column in taskTable.columns:
#                print(len(taskTable_old.loc[:numTasks, column]))
#                print(taskTable.loc[:, column])
#                taskTable_new.loc[:numTasks, column] = taskTable.loc[:, column]
#            taskTable_new.loc[list(range(numTasks, len(columns_old))), :] = ''
            self.emptySheetExcludeHeaders(writer.book['Sheet1'])
            taskTable_new.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False,index=False)
            writer.save()
        
        taskTable = pd.read_excel(self.taskFilePath, sheet_name = 'Sheet1')
        intialTaskIdx = list(taskTable.index)
        random.shuffle(intialTaskIdx)
        if len(intialTaskIdx) > len(self.serverList):
            intialTaskIdx = intialTaskIdx[:len(self.serverList)]
        for i in range(len(intialTaskIdx)):
            taskTable.loc[intialTaskIdx[i], 'HostName'] = self.serverList[i]['name']
        newFileName = os.path.join(self.newTaskFolder, 'TaskList_'+lastModifiedTimeStr+'.json')
        taskTable.to_json(newFileName)
#        return taskTable
#        self.lastModifiedTime = self.getFileLastModifiedTime()
#        lastModifiedTimeStr = datetime.strftime(self.lastModifiedTime, "%Y%m%d_%H%M%S")
#        input_columns = columns + ['UUID']
#        for ServerName in ServerNameList:
#            input_all = []
#            taskIdx = []
#            ServerName = ServerName.lower()
#            for i in range(numTasks):
#                finished = taskTable.loc[i, 'Finished']
#                targetServer = taskTable.loc[i, 'ServerName'].lower()
#                if finished != 1 and targetServer == ServerName:
#                    input_all.append(taskTable.loc[i, input_columns].values.tolist())
#                    taskIdx.append(i)
#            newFileName = os.path.join(self.newTaskFolder, '_'+lastModifiedTimeStr+'_PyPickle.out')
#            oldFileName = os.path.join(self.finishedTaskFolder, '_'+lastModifiedTimeStr+'_PyPickle.out')
#            if len(input_all) > 0 and not os.path.isfile(oldFileName):
#                with open(newFileName, 'wb') as f:
#                    pickle.dump([input_all, taskIdx], f)

if __name__ == '__main__':
    pass
