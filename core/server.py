#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:08 2021

@author: frank
"""

from PyTaskDistributor.util.others import (
        sleepMins, getProcessList,
        getProcessCPU)
from PyTaskDistributor.util.json import (
        writeJSON_from_dict, readJSON_to_df, readJSON_to_dict)
from PyTaskDistributor.core.session import Session
from multiprocessing import Process, Manager
import os
import sys
from datetime import datetime
import numpy as np
import shutil
import matlab.engine
from dirsync import sync
import random
import traceback
from collections import OrderedDict
import psutil

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
        self.manager = Manager()
        self.currentSessions = self.manager.dict()
        self.processes = {}
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
            statusDict['currentSessions'] = []
            statusDict['finishedSessions'] = OrderedDict()
            self.statusDict = statusDict
        self.updateServerStatus()
        self.writeServerStatus()
    
    def onStartTask(self):
        if int(self.statusDict['num_matlab']) == 0:
            self.prepareFactory()
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
                        else:
                            logFile=matFolder = os.path.join(self.matFolderPath, session, session+'.txt')
                            with open(logFile, 'rt') as f:
                                lines = f.read().splitlines()
                                lines = lines[1:]
                                msg = {}
                                msg['Finished'] = 1
                                msg['Comments'] = '|'.join(lines)
                                self.statusDict['finishedSessions'][key] = msg
                            
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
    
    def getUnfinishedSessions(self):
        outputFolder = os.path.join(self.factoryFolder, 'Output')
        unfinishedSessions = os.listdir(outputFolder)
        unfinishedSessions = [session for session in unfinishedSessions if session.startswith('Task-')]
        return unfinishedSessions, outputFolder    
    
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
    
    def createSessions(self, df):
        unfinishedSessions, outputFolder = self.getUnfinishedSessions()
        input_columns = ('Num, totalTime, tauMin, tauMax, wcRatio, maxSD, '
                         'kd1, kd2, kd3, kd4, kd5, A2, A3, A4, kg1, kg2, kg3, '
                         'kg4, initialSaturation, UUID')
        input_columns = input_columns.split(', ')
        sessions = {}
        for i in range(len(df)):
            session = df.index[i]
            if session in unfinishedSessions:
                if session not in self.statusDict['currentSessions']:
                    matFolder = os.path.join(outputFolder, session, 'data')
                    matList = os.listdir(matFolder)
                    if len(matList) > 0:
                        matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
                        targetMatIdx = matModifiedTime.index(max(matModifiedTime))
                        targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
                        sessions[session] = Session(self, session, targetMatPath)
                        continue;
            input = list(df.loc[session, input_columns])
            sessions[session] = Session(self, session, input)
        return sessions
    
    def checkProcesses(self):
        for k, v in self.processes.items():
            if not v.is_alive():
                print("Process for {} is killed".format(k))
    
    def runSessions(self, sessions):
        for k, v in sessions.items():
            p = Process(target=v.main)
            p.start()
            self.processes[k] = p

    def updateSessionsStatus(self):
        for k, v in self.currentSessions.items():
            if k not in self.statusDict['currentSessions']:# add session
                self.statusDict['currentSessions'].append(k)
            if v != 1: # remove session
                self.statusDict['finishedSessions'][k] = v
                self.statusDict['currentSessions'].remove(k)
                del self.currentSessions[k]
                del self.processes[k]
#        print('currentSessions0', self.currentSessions)
#        print('currentSessions1', self.statusDict['currentSessions'])
#        print('finishedSessions', self.statusDict['finishedSessions'])
    
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
    
    def getTaskList(self):
        taskList = [file for file in os.listdir(self.newTaskFolder) if file.endswith(".json")]
        return taskList
    
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
        taskList = self.getTaskList()
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
            sessions = self.createSessions(df)
            self.runSessions(sessions)
            self.updateSessionsStatus()
        
#        time.sleep(10)
        sleepMins(3)# wait for the starting of simulation
        self.updateServerStatus()
        self.writeServerStatus()
    
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