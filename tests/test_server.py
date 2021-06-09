#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

import os
import time
from PyTaskDistributor.util.config import readConfig, getHostName
from PyTaskDistributor.core.server import Server


config_path= os.path.join(os.getcwd(), 'config.txt')

config = readConfig(config_path)
hostname = getHostName()

#hostname = 'cmmb01'
setup = config[hostname]
#setup['order'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
#setup['delivery'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
#setup['factory'] = '/home/frank/LocalWorkFolder/TranReNu/MC3DVersion3.3_Git/'        
obj = Server(setup)
#obj.main()
#taskList = obj.getTaskList()
#task = taskList[0]
#obj.updateTaskFolderPath(task)
#finishedSessions = obj.getFinishedSessions()
#df = obj.getTaskTable(task)# check new task
#df = obj.removeFinishedInputs(df)
#unfinishedSessions, outputFolder = obj.getUnfinishedSessions()
##session = unfinishedSessions[0]
#sessions = obj.createSessions(df)
#obj.runSessions(sessions)
#count = 1
#while True:
#    time.sleep(1)
#    print('-'*30+str(count)+'-'*30)
#    count += 1
#    obj.updateSessionsStatus()
#    obj.checkProcesses()
#k, v  = list(sessions.items())[0]
#session = finishedSessions[0]
#matFolder = os.path.join(obj.matFolderPath, session, 'data')
#matList = os.listdir(matFolder)
#if len(matList) > 0:
#    import pickle
#    import matlab.engine
#    os.chdir(obj.factoryFolder)
#    eng = matlab.engine.start_matlab()
#    matModifiedTime = [os.path.getmtime(os.path.join(matFolder, mat)) for mat in matList]
#    targetMatIdx = matModifiedTime.index(max(matModifiedTime))
#    targetMatPath = os.path.join(matFolder, matList[targetMatIdx])
#    [output, session_name]  = eng.postProcessHandlesV2( targetMatPath, nargout=2)
#    key = obj.taskTimeStr+'_'+session_name
#    obj.statusDict['finishedSessions'][key] = output
#    os.chdir(obj.defaultFolder)            
#    with open('output_PyPickle.out', 'wb') as f:
#        pickle.dump([output, session_name], f)
#    eng.exit()


"""
with open('output_PyPickle.out', 'rb') as f:
    [output, session_name] = pickle.load(f)

"""
#cmd_tempate = "nohup matlab-R2016B -r 'D3Q7Initiator({});exit;' > {}"
#inputs = ['1.0,86400.0,40.0,40.0,0.4,1000000.0,9.0,5.0,8.0,40.0,40.0,40.0,40.0,0.0',
#          'Task-1.txt']

#import psutil


#print(psutil.cpu_percent())
