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
import random
from glob import glob
from PyTaskDistributor.util.others import getFileSuffix, getLatestFileInFolder

class Session:
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
        
        self.initialise()
        
            
    def initialise(self):
        self.makedirs(self.workingFolder)

    
    def createMatlabEng(self, caller):
        print("Creating matlab engine for {} {}".format(caller, self.name))
        try:
            eng = matlab.engine.start_matlab()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print ("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            with open(self.logFile, 'a') as f:
                f.write(str(e)+'\n')
                f.write(traceback.format_exc()+'\n')
            self.server.dealWithFailedSession(self.name)
        print("Creating matlab engine for {} {} done".format(caller,self.name))
        return eng
    
    def getSimulationOutput(self):
        os.chdir(self.factoryFolder)
        eng = self.createMatlabEng('getSimulationOutput')
        print("Loading {}".format(self.input))
        [output, session_name]  = eng.postProcessHandlesV2(\
                        self.input, nargout=2)
        if output['Finished'] == 1.0:
            output['Comments'] = os.path.basename(self.input)
        else:
            msg = "Mark {} with err message {}"\
            .format(self.input, output['err_msg'])
            print(msg)
        eng.exit()
        os.chdir(self.defaultFolder) 
        return output
    
    def markFinishedSession(self):
        try:
            self.server.currentSessions[self.name] = 1
            print("Marking on {}".format(self.name))
            output = self.getSimulationOutput()
            self.server.currentSessions[self.name] = output
            print("Finishing marking {}".format(self.name))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print ("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            self.server.dealWithFailedSession(self.name)
        
    def runMatlabUnfinishedTask(self):
        time.sleep(random.randint(1, 30))
        eng = self.createMatlabEng('old task')
        output, folderName = eng.MatlabToPyRunUnfinishedTasks(self.input, nargout=2)
        return output, folderName
    
    
    def runMatlabNewTask(self):
#        time.sleep(random.randint(60, 120))
        uuid = self.input[-1]
        inputs = [float(i) for i in self.input[:-1]] 
        inputs.append(uuid)
        eng = self.createMatlabEng('new task')
        output, folderName = eng.MatlabToPyRunNewTasks(inputs, nargout=2)
        return output, folderName
    
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
    
    
    def runMatlabTask(self):
        os.chdir(self.factoryFolder)
        input_type = type(self.input)
        if input_type is str:
            output, folderName = self.runMatlabUnfinishedTask()
        else:
            output, folderName = self.runMatlabNewTask()
        sourceFolder = os.path.join(self.factoryFolder, 'Output', folderName)
        matFolder = os.path.join(sourceFolder, 'data')
        matPath  = getLatestFileInFolder(matFolder)
        if 'err_msg' in output:
            output['Comments'] = output['err_msg']
        else:
            if matPath is not None:
                output['Comments'] = os.path.basename(matPath)
            else:
                with open(self.logFile, 'rt') as f:
                    lines = f.read().splitlines()
                    lines = lines[1:]
                    output['Finished'] = 1
                    output['Comments'] = '|'.join(lines)
        targetFolder = os.path.join(self.matFolderPath, folderName)
        #copy simulation results to task result folder
        copy_tree(sourceFolder, targetFolder)
        # delivery everything excluding mat file
        self.deliveryTask(targetFolder)
        self.server.cleanFolder(sourceFolder, 'runMatlabTask in Session', delete=True)
        os.chdir(self.defaultFolder)
        return output
  
    def main(self):
        try:
            self.server.currentSessions[self.name] = 1
            print("Working on {}".format(self.name))
    #        print("Input is {}".format(self.input))
            output = self.runMatlabTask()
            self.server.currentSessions[self.name] = output
            print("Finishing {}".format(self.name))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print ("Need assisstance for unexpected error:\n {}"\
                   .format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            with open(self.logFile, 'a') as f:
                f.write(str(e)+'\n')
                f.write(traceback.format_exc()+'\n')
            self.server.dealWithFailedSession(self.name)
if __name__ == '__main__':
    pass