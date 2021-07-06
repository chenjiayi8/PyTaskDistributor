#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 11:16:30 2021

@author: frank
"""

import os
import sys
import traceback
import time
import shutil
import matlab.engine
from distutils.dir_util import copy_tree
from datetime import datetime
import random
from glob import glob
from PyTaskDistributor.util.others import getFileSuffix, getLatestFileInFolder
from PyTaskDistributor.util.json import readJSON_to_dict
#from PyTaskDistributor.util.extract import extractAfter
from multiprocessing import Process

class Session():
    def __init__(self, server, name, input):
        self.server = server
        self.name = name
        self.input  = input
        self.defaultFolder = server.defaultFolder
        self.mainFolder = server.mainFolder
        self.factoryFolder = server.factoryFolder
        self.workingFolder = os.path.join(self.factoryFolder, 'Output', name)
        self.deliveryFolderPath = server.deliveryFolderPath
        self.matFolderPath = server.matFolderPath
        self.logFile = os.path.join(self.factoryFolder,\
                                    'Output', name, name+'.txt')
        self.process = []
        self.pid = -1
        self.eng = []
        self.output = -1
        self.zombieState = 0
        self.terminated = False
    

    def createLogFile(self):
        if not os.path.isfile(self.logFile):
            basedir = os.path.dirname(self.logFile)
            self.makedirs(basedir)
            with open(self.logFile, 'a'):# touch file
                os.utime(self.logFile, None)
        
    def initialise(self):
        self.makedirs(self.workingFolder)
        self.createLogFile()
        self.writeLog("Session {} initialised".format(self.name))
     
    def writeLog(self, msg):
        time_prefix = datetime.strftime(datetime.now(),\
                                        '%Y-%m-%d %H:%M:%S.%f');
        time_prefix = time_prefix[:-3] + '$ '
        msg = time_prefix+msg+'\n'
        print(msg)
        try:
            if os.path.isfile(self.logFile):
                with open(self.logFile, 'a+') as f:
                    f.write(msg)
        except:
            pass
        
        
    def cleanWorkspace(self, caller=''):
        if not self.terminated:
            if len(caller) > 0:
                self.writeLog('cleanWorkspace called for {} because {}'.format(\
                              self.name, caller))
            else:
                self.writeLog('cleanWorkspace called for {}'.format(self.name))
                
            self.__del__()
            self.terminated = True
            
    def __del__(self):
        if type(self.process) != list:
            self.process.terminate()
            self.writeLog("process terminated")
            if type(self.eng) != list:
                try:
                    self.eng.exit()
                    self.eng = []
                    self.pid = -1
                    self.writeLog("Eng exited")
                except:
                    self.eng = []
                    pass
        if self.pid != -1: #kill if necessary
            try: #try to kill
                exitcode = os.system("kill -9 {}".format(self.pid))
                if exitcode == 0:
                    self.writeLog("kill zombie {} with pid {}".format(\
                          self.name, self.pid))
                    self.pid = -1
                    self.__del__()#make sure terminate process and eng
                else:
                    self.writeLog("Cannot kill {} with pid {}".format(\
                          self.name, self.pid))
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.writeLog("Need assisstance for kill session:\n {}"\
               .format(sys.exc_info()))
                traceBackObj = sys.exc_info()[2]
                traceback.print_tb(traceBackObj)
        
        if self.name in self.server.currentSessions:
            del self.server.currentSessions[self.name]
    
    
    def createMatlabEng(self):
        os.chdir(self.factoryFolder)
        self.initialise()
        if type(self.input) is str:
            caller = 'old task'
        else:
            caller = 'new task'
        self.writeLog("Creating matlab engine for {} {}".format(\
                      caller, self.name))
        try:
            self.eng = matlab.engine.start_matlab()
            self.pid = int(self.eng.eval("feature('getpid')"))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.writeLog("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            self.writeLog(str(e))
            self.writeLog(traceback.format_exc())
            self.server.dealWithFailedSession(self.name)
        self.writeLog("Creating matlab engine for {} {} done".format(\
                      caller,self.name))
    
    def getSimulationOutput(self):
        os.chdir(self.factoryFolder)
        self.writeLog("Loading {}".format(self.input))
        self.eng.postProcessHandlesV2(self.input, nargout=0)
        output = self.readOutput()
        if output['Finished'] == 1.0:
            output['Comments'] = os.path.basename(self.input)
        else:
            msg = "Mark {} with err message {}"\
            .format(self.input, output['err_msg'])
            self.writeLog(msg)
        self.eng.exit()
        os.chdir(self.defaultFolder) 
#        for k, v in output:
#            self.output[k] = v
        self.output = output
    
    def markFinishedSession(self):
        try:
            self.server.currentSessions[self.name] = 1
            self.writeLog("Marking on {}".format(self.name))
            self.getSimulationOutput()
            self.server.currentSessions[self.name] = self.output
            self.writeLog("Finishing marking {}".format(self.name))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.writeLog("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            self.writeLog(traceback.format_exc())
            self.server.dealWithFailedSession(self.name)
        
    def makedirs(self, folder):
        if not os.path.isdir(folder):
            os.makedirs(folder)
    
    def deliveryTask(self, targetFolder):
        targetFolder = os.path.normpath(targetFolder)
        basename = os.path.basename(targetFolder)
        targetFolder += os.path.sep
        itemList = glob(os.path.join(targetFolder,  '**'), recursive=True)
        last_parts = [item.replace(targetFolder, '') for item in itemList]
        for i in range(len(itemList)):
            item = itemList[i]
            path_new = os.path.join(self.deliveryFolderPath,\
                                    basename, last_parts[i])
            if os.path.isdir(item):
                self.makedirs(path_new)
            if os.path.isfile(item):
                if getFileSuffix(item).lower() != '.mat':
                    shutil.copyfile(item, path_new)
    
    
    def getJSONOutput(self):
        dataFolder = os.path.join(self.workingFolder, 'data')
        json_file = getLatestFileInFolder(dataFolder, '.json')
        return json_file
    
    def getMatOutput(self):
        dataFolder = os.path.join(self.workingFolder, 'data')
        mat_file = getLatestFileInFolder(dataFolder, '.mat')
        return mat_file
    
    def readOutput(self):
        json_file = self.getJSONOutput()
        if json_file is not None:
            output = readJSON_to_dict(json_file)
            return output
        else:
            return None
    
    def isWrittenFile(self, file, seconds=30):
        mtime = os.path.getmtime(file)
        interval = 5
        while seconds > 0:
            time.sleep(interval)
            seconds -= interval
            if mtime != os.path.getmtime(file):
                return False
        return True
        
    
    def hasFinished(self):
        json_file = self.getJSONOutput()
        mat_file = self.getMatOutput()
        if json_file is None: return False
        if mat_file is None: return False
        if not self.isWrittenFile(mat_file, 60):
            return False
        if os.path.basename(json_file).lower() == 'final.json':
            return True
        else:
            return False
    
    def runMatlabUnfinishedTask(self):
        time.sleep(random.randint(1, 30))
#        output, folderName = self.eng.MatlabToPyRunUnfinishedTasks(self.input, nargout=2)
        self.eng.MatlabToPyRunUnfinishedTasks(self.input, nargout=0)
        output = self.readOutput()
        return output
    
    
    def runMatlabNewTask(self):
#        time.sleep(random.randint(60, 120))
        uuid = self.input[-1]
        inputs = [float(i) for i in self.input[:-1]] 
        inputs.append(uuid)
#        future = self.eng.MatlabToPyRunNewTasks(inputs,async=True, nargout=2)
#        output, folderName = self.eng.MatlabToPyRunNewTasks(inputs, nargout=2)
        self.eng.MatlabToPyRunNewTasks(inputs, nargout=0)
        output = self.readOutput()
#        output, folderName = future.result()
        return output
    
    
    def runMatlabTask(self):
        os.chdir(self.factoryFolder)
        input_type = type(self.input)
        if input_type is str:
            output = self.runMatlabUnfinishedTask()
        else:
            output = self.runMatlabNewTask()
        self.writeLog("Getting output for {}".format(self.name))
        sourceFolder = os.path.join(self.factoryFolder, 'Output', self.name)
        matFolder = os.path.join(sourceFolder, 'data')
        matPath  = getLatestFileInFolder(matFolder)
#        if 'err_msg' in output:
#            output['Comments'] = output['err_msg']
#        else:
        if matPath is not None:
            output['Comments'] = os.path.basename(matPath)
        else:
            with open(self.logFile, 'rt') as f:
                lines = f.read().splitlines()
                lines = lines[1:]
                output['Finished'] = 1
                output['Comments'] = '|'.join(lines)
#        targetFolder = os.path.join(self.matFolderPath, self.name)
#        #copy simulation results to task result folder
#        copy_tree(sourceFolder, targetFolder)
#        # delivery everything excluding mat file
#        self.deliveryTask(targetFolder)
#        self.server.cleanFolder(sourceFolder, 'runMatlabTask in Session',\
#                                delete=True)
#        os.chdir(self.defaultFolder)
##        for k, v in output:
##            self.output[k] = v
        self.postProcess()
        self.output = output
  
    def postProcess(self):
        targetFolder = os.path.join(self.matFolderPath, self.name)
        sourceFolder = os.path.join(self.factoryFolder, 'Output', self.name)
        #copy simulation results to task result folder
        copy_tree(sourceFolder, targetFolder)
        # delivery everything excluding mat file
        self.writeLog('Ready to delivery result')
        self.deliveryTask(targetFolder)
        self.server.cleanFolder(sourceFolder, 'runMatlabTask in Session',\
                                delete=True)
        os.chdir(self.defaultFolder)
    
    def main(self, target=''):
        if len(target) == 0:
            target = 'run'
        #call function from string
        self.process = Process(target=getattr(self, target))
        self.process.start()
        
    
    def run(self):
        try:
            self.server.currentSessions[self.name] = 1
            self.writeLog("Working on {}".format(self.name))
            self.runMatlabTask()
            self.server.currentSessions[self.name] = self.output
            self.writeLog("Finishing {}".format(self.name))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.writeLog("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            self.writeLog(str(e))
            self.writeLog(traceback.format_exc())
            self.server.dealWithFailedSession(self.name)
if __name__ == '__main__':
    pass