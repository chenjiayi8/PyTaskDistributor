#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 08:56:47 2021

@author: frank
"""
from PyTaskDistributor.util.others import get_uuid


if __name__ == '__main__':
    import pandas as pd
    from datetime import datetime
    import os
    from PyTaskDistributor.util.json import write_json_from_df

    taskFilePath = r'/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/TaskList_manual.xlsx'
    newTaskFolder = r'/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/NewTasks'
    lastModifiedTime = taskTable_modifiedTime = datetime.fromtimestamp(os.path.getmtime(taskFilePath))
    lastModifiedTimeStr = datetime.strftime(lastModifiedTime, "%Y%m%d_%H%M%S")
    newJsonName = os.path.join(newTaskFolder, 'TaskList_' + lastModifiedTimeStr + '.json')
    df = pd.read_excel(taskFilePath)
    df = df.fillna('')
    df.loc[:, 'UUID'] = df.loc[:, 'UUID'].apply(get_uuid)
    write_json_from_df(newJsonName, df)
