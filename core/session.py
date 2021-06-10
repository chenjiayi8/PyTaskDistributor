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

class Session:
    def __init__(self, server, name, input):
        self.server = server
        self.name = name
        self.input  = input
        self.defaultFolder = server.defaultFolder
        self.mainFolder = server.mainFolder
        self.factoryFolder = server.factoryFolder
        self.deliveryFolder = server.deliveryFolder
        self.matFolderPath = server.matFolderPath
        self.logFile = os.path.join(self.matFolderPath, name, name+'.txt')

    def runMatlabUnfinishedTasks(self, input):
        time.sleep(random.randint(30, 60))
        print("Creating matlab engine for {}".format(input))
        eng = matlab.engine.start_matlab()
        print("Have matlab engine for {}".format(input))
        output, outputFolderName = eng.MatlabToPyRunUnfinishedTasks(input, nargout=2)
        return output, outputFolderName
    
    
    def runMatlabNewTasks(self, input):
#        time.sleep(random.randint(60, 120))
        uuid = input[-1]
        inputs = [float(i) for i in input[:-1]] 
        inputs.append(uuid)
        print("Creating matlab engine for {}".format(input))
        eng    = matlab.engine.start_matlab()
        print("Have matlab engine for {}".format(input))
        output, outputFolderName = eng.MatlabToPyRunNewTasks(inputs, nargout=2)
        return output, outputFolderName
    
    def runMatlabTasks(self, input):
        os.chdir(self.factoryFolder)
        input_type = type(input)
        if input_type is str:
            output, outputFolderName = self.runMatlabUnfinishedTasks(input)
        else:
            output, outputFolderName = self.runMatlabNewTasks(input)
        sourceFolder = os.path.join(self.factoryFolder, 'Output', outputFolderName)
        targetFolder = os.path.join(self.matFolderPath, outputFolderName)
        copy_tree(sourceFolder, targetFolder)#copy simulation results to task result folder
        shutil.rmtree(sourceFolder)
        os.chdir(self.defaultFolder)
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
            print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            with open(self.logFile, 'a') as f:
                f.write(str(e))
                f.write(traceback.format_exc())
if __name__ == '__main__':
    pass