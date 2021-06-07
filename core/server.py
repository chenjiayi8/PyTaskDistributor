#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:08 2021

@author: frank
"""

from PyTaskDistributor.util.others import (
        sleepMins, markFinishedTasks, getProcessList)

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
#        self.finishedTaskFolder = os.path.join(self.mainFolder, 'FinishedTasks')
#        self.taskFilePath = os.path.join(self.mainFolder, 'TaskList.xlsx')
#        numCPULimit = int(os.environ.get('OMP_NUM_THREADS'))
#        self.pool = ThreadPool(numCPULimit)
        self.excludedFolder=['Output', 'NewTasks', 'FinishedTasks', '.git']
        self.initialise()
        self.updateServerStatus()
        self.writeServerStatus()
        # unfinished tasks
        # new tasks
        
    def initialise(self):
        statusDict = OrderedDict()
        statusDict['name'] = self.hostName
        statusDict['user'] = self.userName
        statusDict['currentSessions'] = OrderedDict()
        statusDict['finishedSessions'] = OrderedDict()
        self.statusDict = statusDict
    
    def writeServerStatus(self):
        with open(self.status_file, "w+") as outfile: 
            json.dump(self.statusDict, outfile, indent = 4)
    
    def updateServerStatus(self):
        df = getProcessList()
        df_user = df[df['User']==self.userName]
        df_matlab = df_user[df_user['Command'].apply(lambda x: 'matlab' in x.lower())]
        self.statusDict['CPU_total'] = str(round(psutil.cpu_percent(interval=1), 2))
        self.statusDict['Mem_total'] = str(round(sum(df['Mem']), 2))
        if len(df_matlab) > 0:
            df_matlab = df_matlab.reset_index(drop=True)
            #measure the CPU usage of my matlab process
            df_matlab['CPU'] = df_matlab['pid'].apply(lambda p: psutil.Process(p).cpu_percent(interval=1))
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
            os.chdir(self.factoryFolder)
            eng = matlab.engine.start_matlab()
            for session in  sessions:
                matFolder = os.path.join(obj.matFolderPath, session, 'data')
                matList = os.listdir(matFolder)
                if len(matList) > 0:
                    matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                    targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                    targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                    
    
    def getFinishedSessions(self):
        sessions = os.listdir(self.matFolderPath)
        finishedSessions = []
        for session in sessions:
            if session not in self.statusDict['finishedSessions']:
                finishedSessions.append(session)
        return finishedSessions
    
    def removeFinishedInputs(self, df):
        finishedSessions = os.listdir(self.matFolderPath)
        unwantedInputs = []
        for session in finishedSessions:
            if session in df.index:
                unwantedInputs.append(session)
        df2 = df.drop(unwantedInputs)
        return df2
#        for i in range(len(input_all)):
#            inputs = input_all[i]
#            taskname = 'Task-'+str(inputs[0])+'-'+ inputs[-1]
#            if taskname in finishedTasks:
#                unwantedInputs.append(inputs)
#                unwantedTaskIdx.append(taskIdx[i])
#        input_all = [inputs for inputs  in input_all if inputs not in unwantedInputs]
#        taskIdx   = [idx for idx in taskIdx if idx not in unwantedTaskIdx]
#        return input_all, taskIdx
    
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
#        summaryFilePath = os.path.join(self.mainFolder, 'Output',
#                   'TaskList_'+timeStr+'_'+self.hostName+'.xlsx')
#        return taskFolderPath, matFolderPath
    
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
        with open(input_path, 'r') as f:
#                json_data = json.load(f)
            json_str = f.readlines()
            json_str = '\n'.join(json_str)
            json_dict = customdecoder.decode(json_str)
#            json_str = pd.factorize
            df = pd.read_json(json_str)
            df = df[list(json_dict.keys())]
            df = df.sort_values('Num')
            temp = list(zip(['Task']*len(df), df['Num'].apply(str) , df['UUID']))
            index = ['-'.join(t) for t in temp]
            df.index = index
            return df
    
    def runSimulation(self):
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
            df = self.getTaskTable(task)
            df = self.removeFinishedInputs(df)
#            output_path = os.path.join(self.finishedTaskFolder, task)
            print("Working on {}".format(task))
            # sync folder to make sure new implementation is avaiable
            self.prepareFactory()
            # create task folder in factory folder
            self.updateTaskFolderPath(task)
#            self.taskFolderPath = taskFolderPath
#            if os.path.exists(taskFolderPath):
#                shutil.rmtree(taskFolderPath)
            if not os.path.exists(self.taskFolderPath):
                os.makedirs(self.taskFolderPath)
            # copy task file to task folder
#            copiedTaskFile = os.path.join(self.taskFolderPath, 'TaskList.xlsx')
#            if not os.path.isfile(copiedTaskFile)
#                shutil.copyfile(self.taskFilePath, copiedTaskFile)
            
            # cd to factory folder to start the simulation
            os.chdir(self.factoryFolder)
            # load inputs and run simulation
            df = pd.read_json(input_path)
            with open(input_path, 'rb') as f:
                [input_all, taskIdx] = pickle.load(f)
                # remove finished inputs
                input_all, taskIdx = self.removeFinishedInputs(input_all, taskIdx)
                # buil unfinished tasks
                input_all = self.buildUnfinishedInputs(input_all)
#                results_old = self.pool.map(self.runMatlabUnfinishedTasks, unfinishedMats)
                # run rest inputs
                results = self.pool.map(self.runMatlabTasks, input_all)
#                print(input_all)
#                results = []
#                for inputs in input_all:
#                    result = self.callMatlab(inputs)
#                    results.append(result)
                taskTable = pd.read_excel(copiedTaskFile, sheet_name = 'Sheet1')  
                taskTable = markFinishedTasks(taskTable, results, taskIdx)
                self.writeTasktable(copiedTaskFile, taskTable)
                shutil.move(input_path, output_path)#remove new task to finished
                

            if not os.path.isfile(self.summaryFilePath):
                print(copiedTaskFile)
                print(self.summaryFilePath)
                shutil.copyfile(copiedTaskFile, self.summaryFilePath)
#                sourceFolder = os.path.join(self.factoryFolder, 'Output')
#                copy_tree(sourceFolder, self.taskFolderPath)
#                self.cleanFolder(sourceFolder)
            # cd back to default folder for next simulation
            os.chdir(self.defaultFolder)
    
    def main(self):
        numMin = random.randint(3,10)
        try:
            self.runSimulation()
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