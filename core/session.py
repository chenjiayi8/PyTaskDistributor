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
from PyTaskDistributor.util.others import getFileSuffix

class Session:
    def __init__(self, server, name, input):
        self.server = server
        self.name = name
        self.input  = input
        self.defaultFolder = server.defaultFolder
        self.mainFolder = server.mainFolder
        self.factoryFolder = server.factoryFolder
        self.deliveryFolderPath = server.deliveryFolderPath
        self.matFolderPath = server.matFolderPath
        self.logFile = os.path.join(self.factoryFolder,\
                                    'Output', name, name+'.txt')

    def runMatlabUnfinishedTasks(self, input):
        time.sleep(random.randint(30, 60))
#        print("Creating matlab engine for {}".format(input))
        eng = matlab.engine.start_matlab()
#        print("Have matlab engine for {}".format(input))
        output, folderName = eng.MatlabToPyRunUnfinishedTasks(input, nargout=2)
        return output, folderName
    
    
    def runMatlabNewTasks(self, input):
#        time.sleep(random.randint(60, 120))
        uuid = input[-1]
        inputs = [float(i) for i in input[:-1]] 
        inputs.append(uuid)
#        print("Creating matlab engine for {}".format(input))
        eng    = matlab.engine.start_matlab()
#        print("Have matlab engine for {}".format(input))
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
    
    
    def runMatlabTasks(self, input):
        os.chdir(self.factoryFolder)
        input_type = type(input)
        if input_type is str:
            output, folderName = self.runMatlabUnfinishedTasks(input)
        else:
            output, folderName = self.runMatlabNewTasks(input)
        sourceFolder = os.path.join(self.factoryFolder, 'Output', folderName)
        targetFolder = os.path.join(self.matFolderPath, folderName)
        #copy simulation results to task result folder
        copy_tree(sourceFolder, targetFolder)
        # delivery everything excluding mat file
        self.deliveryTask(targetFolder)
        shutil.rmtree(sourceFolder)
        os.chdir(self.deglobfaultFolder)
        return output
  
    def main(self):
        try:
            self.server.currentSessions[self.name] = 1
            print("Working on  {}".format(self.name))
    #        print("Input is {}".format(self.input))
            output = self.runMatlabTasks(self.input)
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
                f.write(str(e))
                f.write(traceback.format_exc())
if __name__ == '__main__':
    pass