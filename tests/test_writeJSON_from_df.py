#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 08:56:47 2021

@author: frank
"""
import random
import uuid

def getUUID(origin):
    rd = random.Random()
#    rd.seed(0)
    uuid_str = str(uuid.UUID(int=rd.getrandbits(128)))
    return uuid_str[:5]

if __name__ == '__main__':
    import pandas as pd
    from datetime import datetime
    import os
    from PyTaskDistributor.util.json import writeJSON_from_df
    taskFilePath=r'/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/TaskList_manual.xlsx'
    newTaskFolder=r'/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/NewTasks'
    lastModifiedTime = taskTable_modifiedTime = datetime.fromtimestamp(\
                            os.path.getmtime(taskFilePath))
    lastModifiedTimeStr = datetime.strftime(lastModifiedTime, "%Y%m%d_%H%M%S")
    newJsonName = os.path.join(newTaskFolder,'TaskList_'+lastModifiedTimeStr+'.json')
    df = pd.read_excel(taskFilePath)
    df = df.fillna('')
    df.loc[:, 'UUID'] = df.loc[:, 'UUID'].apply(getUUID)
    writeJSON_from_df(newJsonName, df)
    
    
