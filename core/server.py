#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:08 2021

@author: frank
"""

from PyTaskDistributor.util.others import (
        sleepMins, getProcessList, getLatestFileInFolder, getProcessCPU)
from PyTaskDistributor.util.json import (
         readJSON_to_df, readJSON_to_dict)
from PyTaskDistributor.core.session import Session
from multiprocessing import Process, Manager
import os
import sys
#import time
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
        self.CPU_max = float(setup['CPU_max'])*100
        self.MEM_max = float(setup['MEM_max'])*100
        self.defaultFolder = os.getcwd()
        self.serverFolder = os.path.join(self.defaultFolder, 'Servers')
        self.hostName = self.setup['hostname']
        self.userName = os.popen('whoami').readline().split('\n')[0]
        if not os.path.isdir(self.serverFolder):
            os.makedirs(self.serverFolder)
        self.status_file = os.path.join(self.serverFolder,
                                        self.hostName+'.json')
        self.mainFolder = self.setup['order']
        self.factoryFolder = self.setup['factory']
        self.deliveryFolder = self.setup['delivery']
        self.newTaskFolder = os.path.join(self.mainFolder, 'NewTasks')
        self.finishedTaskFolder = os.path.join(self.mainFolder,
                                               'FinishedTasks')
        self.excludedFolder=['Output', 'NewTasks', 'FinishedTasks', '.git']
        self.manager = Manager()
        self.currentSessions = self.manager.dict()
        self.processes = {}
        self.status_names = ['CPU_total', 'MEM_total',
                             'CPU_matlab', 'MEM_matlab']
        self.status_record = np.zeros(len(self.status_names)+1, dtype=float)
        self.initialise()

    
    def initialise(self):
        if os.path.isfile(self.status_file):
            self.statusDict = readJSON_to_dict(self.status_file)
            #update config
            self.statusDict['name'] = self.hostName
            self.statusDict['user'] = self.userName
            self.statusDict['CPU_max'] = self.CPU_max
            self.statusDict['MEM_max'] = self.MEM_max
        else:
            statusDict = OrderedDict()
            statusDict['name'] = self.hostName
            statusDict['user'] = self.userName
            statusDict['CPU_max'] = self.CPU_max
            statusDict['MEM_max'] = self.MEM_max
            statusDict['msg'] = ''
            statusDict['currentSessions'] = []
            statusDict['finishedSessions'] = OrderedDict()
            self.statusDict = statusDict
        self.recordStatus()
        self.writeServerStatus()
    
    def onStartTask(self):
        if int(self.statusDict['num_matlab']) == 0:
            self.prepareFactory()
        sessions = self.getFinishedSessions()
        if sessions:
            self.markFinishedSession(sessions)
            self.writeServerStatus()
    
    def writeServerStatus(self):
        #calculate the average CPU/MEM percents and write
        averages = self.status_record[:-1]/self.status_record[-1]
        for i in range(len(self.status_names)):
            self.statusDict[self.status_names[i]] = averages[i]
#        writeJSON_from_dict(self.status_file, self.statusDict)
        self.status_record = np.zeros(len(self.status_names)+1, dtype=float)
    
    def recordStatus(self):
        self.updateServerStatus()
        state = [float(self.statusDict[s]) for s in self.status_names]
        self.status_record[:-1] += state
        self.status_record[-1] += 1
        pass
    
    def updateServerStatus(self):
        df = getProcessList()
        df_user = df[df['User']==self.userName]
        #only count matlab process opened by Python
        df_matlab = df_user[df_user['Command'].apply(\
                            lambda x: 'matlab -mvminputpipe' in x.lower())]
        self.statusDict['CPU_total'] = round(psutil.cpu_percent(interval=1), 2)
        self.statusDict['MEM_total'] = round(sum(df['Mem']), 2)
        if len(df_matlab) > 0:
            df_matlab = df_matlab.reset_index(drop=True)
            #measure the CPU usage of matlab process
            df_matlab['CPU'] = df_matlab['pid'].apply(getProcessCPU)
            self.statusDict['num_matlab'] = len(df_matlab)
            self.statusDict['CPU_matlab'] = \
                        round(sum(np.array(df_matlab['CPU'])/100), 2)
            self.statusDict['MEM_matlab'] = round(sum(df_matlab['Mem']), 2)
        else:
            self.statusDict['num_matlab'] = 0
            self.statusDict['CPU_matlab'] = 0.0
            self.statusDict['MEM_matlab'] = 0.0
            
        self.statusDict['updated_time'] = datetime.now().isoformat()
    
    def markFinishedSession(self, sessions):
        if len(sessions) > 0:
            targetMatPaths = []
            for session in sessions:
                key = self.taskTimeStr+'_'+session
                if key not in self.statusDict['finishedSessions']:
                    matFolder = os.path.join(\
                                     self.matFolderPath, session, 'data')
                    matPath = getLatestFileInFolder(matFolder)
                    if matPath:
                        targetMatPaths.append(matPath)
                    else:
                        logFile = os.path.join(self.matFolderPath,\
                                               session, session+'.txt')
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
                    [output, session_name]  = eng.postProcessHandlesV2(\
                                    targetMatPath, nargout=2)
                    key = self.taskTimeStr+'_'+session_name
                    if output['Finished'] == 1.0:
                        output['Comments'] = os.path.basename(targetMatPath)
                        self.statusDict['finishedSessions'][key] = output
                        print("{} is marked".format(key))
                    else:
                        msg = "Mark {} with err message {}\n"\
                        .format(targetMatPath, output['err_msg'])
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
        unfinishedSs = os.listdir(outputFolder)
        unfinishedSs = [s for s in unfinishedSs if s.startswith('Task-')]
        return unfinishedSs, outputFolder    
    
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
        input_columns = ('Num, totalTime, tauMin, tauMax, wcRatio, maxSD,'
                         'kd1, kd2, kd3, kd4, kd5, A2, A3, A4, kg1, kg2, kg3,'
                         'kg4, initialSaturation, UUID')
        input_columns = input_columns.replace(' ', '')
        input_columns = input_columns.split(',')
        sessions = {}
        for i in range(len(df)):
            session = df.index[i]
            if session in unfinishedSessions:#ran before
                if session not in self.currentSessions:#not running now
                    matFolder = os.path.join(outputFolder, session, 'data')
                    matPath = getLatestFileInFolder(matFolder)
                    if matPath:#Progress is saved
                        sessions[session] = Session(self, session, matPath)
                    else:#Nothing is saved
                        input = list(df.loc[session, input_columns])
                        sessions[session] = Session(self, session, input)
            else:# new session
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
            # check process status
            if k not in self.processes:
                self.statusDict['msg'].append(\
                               '{} is not in Processes'.format(k))
            else:
                p = self.processes[k]
                if not p.is_alive():
                    if p.exitcode != 0:
                        self.statusDict['msg'].append(
                        '{} is exited with exitcode {}'.format(k, p.exitcode))
            # add session
            if k not in self.statusDict['currentSessions']:
                self.statusDict['currentSessions'].append(k)
            if v != 1: # remove session
                self.statusDict['finishedSessions'][k] = v
                self.statusDict['currentSessions'].remove(k)
                del self.currentSessions[k]
                del self.processes[k]
        self.removeFinishedSessionsStatus()
    
    def removeFinishedSessionsStatus(self):
        taskList = self.getTaskList(folder=self.finishedTaskFolder)
        sessions = list(self.statusDict['finishedSessions'].keys())
        for task in taskList:
            timeStr = self.getTimeStr(task)
            for session in sessions:
                if timeStr in session:
#                    print("Removing finished session {}".format(session))
                    del self.statusDict['finishedSessions'][session]
    
    def getTimeStr(self, task):
        markLocation = [i for i, ltr in enumerate(task) if ltr == '_']
        dotLocation = [i for i, ltr in enumerate(task) if ltr == '.']
        timeStr = task[markLocation[0]+1:dotLocation[-1]]
        return timeStr
    
    def updateFolderPaths(self, task):
        timeStr = self.getTimeStr(task)
        self.deliveryFolderPath = os.path.join(\
                                   self.deliveryFolder, 'Output', timeStr)
        self.matFolderPath = os.path.join(\
                                  self.factoryFolder, 'Output', timeStr)
        self.taskTimeStr = timeStr
        self.makedirs(self.deliveryFolderPath)
        self.makedirs(self.matFolderPath)
    
    def makedirs(self, folder):
        if not os.path.isdir(folder):
            os.makedirs(folder)
    
    def cleanUnfinishedSessions(self):
        self.prepareFactory()
        unfinishedSessions, outputFolder = self.getUnfinishedSessions()
        for session in unfinishedSessions:
            folder_path = os.path.join(outputFolder, session)
            self.cleanFolder(folder_path)
            shutil.rmtree(folder_path)
        self.currentSessions.clear()
        self.statusDict['currentSessions'].clear()
    
    def cleanFolder(self, folerName):
        if os.path.isdir(folerName):
            for filename in os.listdir(folerName):
                file_path = os.path.join(folerName, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
    
    def prepareFactory(self):
        sync(self.mainFolder, self.factoryFolder, 'sync',\
             create=True, exclude=self.excludedFolder)
    
    def getTaskList(self, folder=None):
        if folder == None:
            folder = self.newTaskFolder
        taskList = [f for f in os.listdir(folder) if f.endswith(".json")]
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
            self.cleanUnfinishedSessions()
            taskList.remove(cleanTask)
            os.unlink(os.path.join(self.newTaskFolder, cleanTask))
        for task in taskList:
            if '_clean.json' in task:
                continue #skip clean task for another server
            # announce the start of simulation
            print("Working on {}".format(task))
            self.updateFolderPaths(task)#paths for output
            self.onStartTask()
            df = self.getTaskTable(task)# check new task
            df = self.removeFinishedInputs(df)
            sessions = self.createSessions(df)
            self.runSessions(sessions)
        
        # wait for the starting of simulation
        sleepMins(1)
        self.updateSessionsStatus()
    
    def main(self):
        numMin = random.randint(3,5)
        interval_seconds = 30
        interval_mins = interval_seconds/60
        num_interval = round(numMin*60/interval_seconds)
        try:
            self.onInterval()
            nowTimeStr = datetime.strftime(\
                               datetime.now(),  "%H:%M:%S %d/%m/%Y")
            msg = "{}: Sleeping for {} mins".format(nowTimeStr, numMin)
#            print(msg)
            print("\r", msg, end='')
            
            for i in range(num_interval):
                self.recordStatus()
                sleepMins(interval_mins)
            self.writeServerStatus()
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

if __name__ == '__main__':
    pass