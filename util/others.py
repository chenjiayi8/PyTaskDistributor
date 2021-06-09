#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:41:42 2021

@author: frank
"""
import os
import time
import psutil
import pandas as pd
from openpyxl import load_workbook

def sleepMins(numMin): #for KeyboardInterrupt 
    for i in range(numMin*60):
        time.sleep(1)
        
def emptySheetExcludeHeaders(ws):
    for i, row in enumerate(ws.iter_rows()):     
        if i != 0:                      
            for cell in row:
                cell.value = None
    
def updateXlsxFile(path_xlsx, df, sheet_name='Sheet1'):
    if not os.path.isfile(path_xlsx):
        df.to_excel(path_xlsx, index=False)
    else:
        book = load_workbook(path_xlsx)
        writer = pd.ExcelWriter(path_xlsx, engine='openpyxl')
        writer.book = book
        writer.sheets = {ws.title: ws for ws in book.worksheets}
        emptySheetExcludeHeaders(writer.book[sheet_name])
        df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False,index=False)
        writer.save() 
    
def markFinishedTasks(taskTable, results=[], taskIdx=[]):
#    numTasks = len(taskTable)
    for i in range(len(taskIdx)):
        idx = taskIdx[i]
        result = results[i]
        for key in result.keys():
            taskTable.loc[idx, key] = result[key]            
    return taskTable


def getProcessList():
    data = []
    columns = ['pid', 'User', 'Name', 'CPU', 'Mem', 'Command']
    for proc in psutil.process_iter():
        try:
            # Get process name & pid from process object.
#            cpu_percent = proc.cpu_percent(interval=0.01)
#            print(cpu_percent)
            data.append([proc.pid, proc.username(), proc.name(),
                     0, proc.memory_percent(), proc.exe()])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return pd.DataFrame(data=data, columns=columns)


def getProcessCPU(pid):
    try:
        return psutil.Process(pid).cpu_percent(interval=1)
    except:
        return 0.0

def sendEmail(subject, content, receiverEmail ="j.chen-2@tudelft.nl", imagePath=None):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.image import MIMEImage
    from email.mime.text import MIMEText
    import os
    senderEmail = os.environ['senderEmail']
    senderPass  = os.environ['senderPass']
    server = smtplib.SMTP('smtp.gmail.com', 587)
    
    #Next, log in to the server
    server.starttls()
    server.login(senderEmail, senderPass)
    
    #Send the mail
    msg = MIMEMultipart()
    msg['From'] = senderEmail
    msg['To'] = receiverEmail
    msg['Subject'] = subject
    
    
    body = content
    msg.attach(MIMEText(body, 'plain'))
    if imagePath != None:
        img_data = open(imagePath, 'rb').read()
        image = MIMEImage(img_data, name=os.path.basename(imagePath))
        msg.attach(image)
    text = msg.as_string()
    server.sendmail(senderEmail,receiverEmail, text)
