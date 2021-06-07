#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:59:40 2020

@author: frank
"""

#import sys
from PyTaskDistributor.util.extract import extractBetween
import platform

def getHostName():
    return platform.uname()[1].lower()

def readConfig(config_path):
    with open(config_path, 'r') as f:
        configLines = f.readlines()
        configLines = [line.replace('\n', '') for line in configLines]
        leftMarkLocations = [i for i in range(len(configLines)) if configLines[i].find('[')==0]
        configList = []
        for i in range(len(leftMarkLocations)-1):
            start = leftMarkLocations[i]
            end   = leftMarkLocations[i+1]
            configList.append(configLines[start:end])
        else:
            start = leftMarkLocations[i+1]
            end   = len(configLines)
            configList.append(configLines[start:end])
        config = {}
        for group in configList:
            tempDict = {}
            for i in range(len(group)):
                item = group[i]
                if i == 0 : 
                    tempDict['role'] = extractBetween(item, '[', ']')[0]
                else:
                    itemList= item.split(':')
                    tempDict[itemList[0]]=itemList[1]
            config[tempDict['hostname']] = tempDict
        return config

if __name__ == "__main__":
    pass