#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:08 2021

@author: frank
"""

from PyTaskDistributor.util.others import (
        sleepMins, markFinishedTasks, getProcessList,
        getProcessCPU)
from PyTaskDistributor.util.json import (
        writeJSON_from_dict, readJSON_to_df, readJSON_to_dict)
import os
import sys
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
import time
import numpy as np
import pandas as pd
import shutil
import pickle
import matlab.engine
from openpyxl import load_workbook
from dirsync import sync
from distutils.dir_util import copy_tree
import random
import traceback
import subprocess
from collections import OrderedDict
import json  
import psutil
from json import JSONDecoder
customdecoder = JSONDecoder(object_pairs_hook=OrderedDict)

class Server:
    def __init__(self, setup):
        self.setup = setup
        self.CPU_percent_max = float(setup['CPU_max'])*100
        self.MEM_percent_max = float(setup['MEM_max'])*100
        self.defaultFolder = os.getcwd()
        self.serverFolder = os.path.join(self.defaultFolder, 'Servers')
        self.hostName = self.setup['hostname']
        self.userName = os.popen('whoami').readline().split('\n')[0]
        if not os.path.isdir(self.serverFolder):
            os.makedirs(self.serverFolder)
        self.status_file = os.path.join(self.serverFolder, self.hostName+'.json')
        self.mainFolder = self.setup['order']
        self.factoryFolder = self.setup['factory']
        self.deliveryFolder = self.setup['delivery']
        self.newTaskFolder = os.path.join(self.mainFolder, 'NewTasks')
        self.excludedFolder=['Output', 'NewTasks', 'FinishedTasks', '.git']
        self.initialise()

    
    def initialise(self):
        if os.path.isfile(self.status_file):
            self.statusDict = readJSON_to_dict(self.status_file)
        else:
            statusDict = OrderedDict()
            statusDict['name'] = self.hostName
            statusDict['user'] = self.userName
            statusDict['CPU_max'] = self.CPU_percent_max
            statusDict['MEM_max'] = self.MEM_percent_max
            statusDict['msg'] = ''
            statusDict['currentSessions'] = OrderedDict()
            statusDict['finishedSessions'] = OrderedDict()
            self.statusDict = statusDict
        self.updateServerStatus()
        self.writeServerStatus()
            
    
    def onStartTask(self):
        sessions = self.getFinishedSessions()
        if sessions:
            self.markFinishedSession(sessions)
            self.writeServerStatus()
    
    def writeServerStatus(self):
        writeJSON_from_dict(self.status_file, self.statusDict)
    
    def updateServerStatus(self):
        df = getProcessList()
        df_user = df[df['User']==self.userName]
        df_matlab = df_user[df_user['Command'].apply(lambda x: 'matlab' in x.lower())]
        self.statusDict['CPU_total'] = str(round(psutil.cpu_percent(interval=1), 2))
        self.statusDict['MEM_total'] = str(round(sum(df['Mem']), 2))
        if len(df_matlab) > 0:
            df_matlab = df_matlab.reset_index(drop=True)
            #measure the CPU usage of my matlab process
            df_matlab['CPU'] = df_matlab['pid'].apply(getProcessCPU)
            self.statusDict['num_matlab'] = str(len(df_matlab))
            self.statusDict['CPU_matlab'] = str(round(sum(np.array(df_matlab['CPU'])/100), 2))
            self.statusDict['MEM_matlab'] = str(round(sum(df_matlab['Mem']), 2))
        else:
            self.statusDict['num_matlab'] = '0'
            self.statusDict['CPU_matlab'] = '0'
            self.statusDict['MEM_matlab'] = '0'
            
        self.statusDict['updated_time'] = datetime.now().isoformat()
    
    def markFinishedSession(self, sessions):
        if len(sessions) > 0:
            targetMatPaths = []
            for session in sessions:
                key = self.taskTimeStr+'_'+session
                if key not in self.statusDict['finishedSessions']:
                    matFolder = os.path.join(self.matFolderPath, session, 'data')
                    if os.path.isdir(matFolder):
                        matList = os.listdir(matFolder)
                        if len(matList) > 0:
                            matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                            targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                            targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                            targetMatPaths.append(targetMatPath)
            if len(targetMatPaths) > 0:
                os.chdir(self.factoryFolder)
                eng = matlab.engine.start_matlab()
                for targetMatPath in targetMatPaths:
                    print("Loading {}".format(targetMatPath))
                    [output, session_name]  = eng.postProcessHandlesV2(targetMatPath, nargout=2)
                    key = self.taskTimeStr+'_'+session_name
                    if output['Finished'] == 1.0:
                        output['Comments'] = os.path.basename(targetMatPath)
                        self.statusDict['finishedSessions'][key] = output
                        print("{} is marked".format(key))
                    else:
                        msg = "Mark {} with err message {}\n".format(targetMatPath, output['err_msg'])
                        self.statusDict['msg'] += msg
                        print(msg)
                os.chdir(self.defaultFolder) 
                eng.exit()
                    
    
    def getFinishedSessions(self):
        if os.path.isdir(self.matFolderPath):
            sessions = os.listdir(self.matFolderPath)
            finishedSessions = []
            for session in sessions:
                key = self.taskTimeStr+'_'+session
                if key not in self.statusDict['finishedSessions']:
                    finishedSessions.append(session)
            return finishedSessions
        else:
            return None
    
    def getRunningSessions(self):
        outputFolder = os.path.join(self.factoryFolder, 'Output')
        runningSessions = os.listdir(outputFolder)
        runningSessions = [session for session in runningSessions if session.startswith('Task-')]
        return runningSessions, outputFolder
    
    def getUnfinishedMatPaths(self, df):
        runningSessions, outputFolder = self.getRunningSessions()
        unfinishedMatPaths = []
        unwantedInputs = []
        for session in runningSessions:
            if session not in self.statusDict['currentSessions']:
                matFolder = os.path.join(outputFolder, session, 'data')
                matList = os.listdir(matFolder)
                if len(matList) > 0:
                    matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                    targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                    targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                    unfinishedMatPaths.append(targetMatPath)
                    if session in df.index:
                        unwantedInputs.append(session)
        df2 = df.drop(unwantedInputs)
        return df2, unfinishedMatPaths
    
    
    def removeFinishedInputs(self, df):
        if len(df) == 0:
            return df
        finishedSessions = os.listdir(self.matFolderPath)
        unwantedInputs = []
        for session in finishedSessions:
            if session in df.index:
                unwantedInputs.append(session)
        df2 = df.drop(unwantedInputs)
        return df2
    
    def createInputs(self, df):
        runningSessions, outputFolder = self.getRunningSessions()
        inputs = []
        unfinishedMatPaths = []
        unwantedInputs = []
        for i in range(len(df)):
            session = df.index(i)
        for session in runningSessions:
            if session not in self.statusDict['currentSessions']:
                matFolder = os.path.join(outputFolder, session, 'data')
                matList = os.listdir(matFolder)
                if len(matList) > 0:
                    matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                    targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                    targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                    unfinishedMatPaths.append(targetMatPath)
                    if session in df.index:
                        unwantedInputs.append(session)
        df2 = df.drop(unwantedInputs)
        return df2, unfinishedMatPaths
    
    def buildUnfinishedInputs(self, input_all):
        outputFolder = os.path.join(self.factoryFolder, 'Output')
        unfinishedTasks = os.listdir(outputFolder)
        for i in range(len(input_all)):
            inputs = input_all[i]
            taskname = 'Task-'+str(inputs[0])+'-'+ inputs[-1]
            if taskname in unfinishedTasks:
                matFolder = os.path.join(outputFolder, taskname, 'data')
                matList = os.listdir(matFolder)
                if len(matList) > 0:
                    matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                    targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                    targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                    input_all[i] = [targetMatPath, taskname]
        return input_all
    
    def runMatlabUnfinishedTasks(self, inputs):
        time.sleep(random.randint(1, 60))
        eng    = matlab.engine.start_matlab()
        output, outputFolderName = eng.MatlabToPyRunUnfinishedTasks(inputs, nargout=2)
        return output, outputFolderName
    
    
    def runMatlabNewTasks(self, inputs):
        uuid = inputs[-1]
        inputs = [float(i) for i in inputs[:-1]] 
        inputs.append(uuid)
        eng    = matlab.engine.start_matlab()
        output, outputFolderName = eng.MatlabToPyRunNewTasks(inputs, nargout=2)
        return output, outputFolderName
    
    def runMatlabTasks(self, inputs):
        input_type = type(inputs[0])
        if input_type is str:
            output, outputFolderName = self.runMatlabUnfinishedTasks(inputs)
        else:
            output, outputFolderName = self.runMatlabNewTasks(inputs)
        sourceFolder = os.path.join(self.factoryFolder, 'Output', outputFolderName)
        targetFolder = os.path.join(self.matFolderPath, outputFolderName)
        copy_tree(sourceFolder, targetFolder)#copy simulation results to task result folder
        shutil.rmtree(sourceFolder)
        return output
    
    def writeTasktable(self, taskFilePath, taskTable):
        book = load_workbook(taskFilePath)
        writer = pd.ExcelWriter(taskFilePath, engine='openpyxl')
        writer.book = book
        writer.sheets = {ws.title: ws for ws in book.worksheets}
        taskTable.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False,index=False)
        writer.save()
    
    def updateTaskFolderPath(self, task):
        markLocation = [i for i, ltr in enumerate(task) if ltr == '_']
        dotLocation = [i for i, ltr in enumerate(task) if ltr == '.']
        timeStr = task[markLocation[0]+1:dotLocation[-1]]
        self.taskFolderPath = os.path.join(self.deliveryFolder, 'Output', timeStr)
        self.matFolderPath = os.path.join(self.factoryFolder, 'Output', timeStr)
        self.taskTimeStr = timeStr
    
    def cleanFolder(self, folerName):
        if os.path.isdir(folerName):
            for filename in os.listdir(folerName):
                file_path = os.path.join(folerName, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
    
    def prepareFactory(self):
        sync(self.mainFolder, self.factoryFolder, 'sync', create=True, exclude=self.excludedFolder)
    
    def getTaskTable(self, task):
        input_path = os.path.join(self.newTaskFolder, task)
        df = readJSON_to_df(input_path)
        df = df.sort_values('Num')
        temp = list(zip(['Task']*len(df), df['Num'].apply(str) , df['UUID']))
        index = ['-'.join(t) for t in temp]
        df.index = index
        df = df.fillna('')
        df2 = df[df['HostName']==self.hostName]
        return df2
    
    def onInterval(self):
        taskList = [file for file in os.listdir(self.newTaskFolder) if file.endswith(".json")]
        cleanTask = self.hostName+'_clean.json'
        # clean previous task results
        if cleanTask in taskList:
            for folder in self.excludedFolder:
                self.cleanFolder(os.path.join(self.factoryFolder, folder))
            taskList.remove(cleanTask)
            os.unlink(os.path.join(self.newTaskFolder, cleanTask))
        for task in taskList:
            # announce the start of simulation
            print("Working on {}".format(task))
            self.updateTaskFolderPath(task)#paths for output
            self.onStartTask()
            df = self.getTaskTable(task)# check new task
            df = self.removeFinishedInputs(df)
#            inputs = self.createInputs(df)
#            inputs = self.createInputs(df)
#            inputs += unfinishedMatPaths
            
            # create process to run individual task on background
            # import multiprocessing as mp
            # Process: p1 = mp.Process(target=keepIndexUpdated) p1.start()
            
#            if len(df) == 0: #no
#                break
#            output_path = os.path.join(self.finishedTaskFolder, task)

            # sync folder to make sure new implementation is avaiable
#            self.prepareFactory()
            # create task folder in factory folder
            
#            self.taskFolderPath = taskFolderPath
#            if os.path.exists(taskFolderPath):
#                shutil.rmtree(taskFolderPath)
            if not os.path.exists(self.taskFolderPath):
                os.makedirs(self.taskFolderPath)
                

            '''
            TODO: 
                1. build inputs for nwe task
                2. build inputs for unfinished task
                3. update finishedSession after finishing a task
            '''
        
        time.sleep(1)
#        sleepMins(3)# wait for the starting of simulation
        self.updateServerStatus()
        self.writeServerStatus()
            # copy task file to task folder
#            copiedTaskFile = os.path.join(self.taskFolderPath, 'TaskList.xlsx')
#            if not os.path.isfile(copiedTaskFile)
#                shutil.copyfile(self.taskFilePath, copiedTaskFile)
            
            # cd to factory folder to start the simulation
#            os.chdir(self.factoryFolder)
#            # load inputs and run simulation
#            df = pd.read_json(input_path)
#            with open(input_path, 'rb') as f:
#                [input_all, taskIdx] = pickle.load(f)
#                # remove finished inputs
#                input_all, taskIdx = self.removeFinishedInputs(input_all, taskIdx)
#                # buil unfinished tasks
#                input_all = self.buildUnfinishedInputs(input_all)
##                results_old = self.pool.map(self.runMatlabUnfinishedTasks, unfinishedMats)
#                # run rest inputs
#                results = self.pool.map(self.runMatlabTasks, input_all)
##                print(input_all)
##                results = []
##                for inputs in input_all:
##                    result = self.callMatlab(inputs)
##                    results.append(result)
#                taskTable = pd.read_excel(copiedTaskFile, sheet_name = 'Sheet1')  
#                taskTable = markFinishedTasks(taskTable, results, taskIdx)
#                self.writeTasktable(copiedTaskFile, taskTable)
#                shutil.move(input_path, output_path)#remove new task to finished
#                
#
#            if not os.path.isfile(self.summaryFilePath):
#                print(copiedTaskFile)
#                print(self.summaryFilePath)
#                shutil.copyfile(copiedTaskFile, self.summaryFilePath)
#                sourceFolder = os.path.join(self.factoryFolder, 'Output')
#                copy_tree(sourceFolder, self.taskFolderPath)
#                self.cleanFolder(sourceFolder)
            # cd back to default folder for next simulation

#            os.chdir(self.defaultFolder)
    
    def main(self):
        numMin = random.randint(3,10)
        try:
            self.onInterval()
            nowTimeStr = datetime.strftime(datetime.now(),  "%H:%M:%S %d/%m/%Y")
            msg = "{}: Sleeping for {} mins".format(nowTimeStr, numMin)
            print(msg)
            print("\r", msg, end='')
#            print(msg)
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

if __name__ == '__main__':
    pass