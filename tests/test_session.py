#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 11:20:47 2021

@author: frank
"""


import os
import time
from PyTaskDistributor.util.config import readConfig, getHostName
from PyTaskDistributor.core.server import Server
from PyTaskDistributor.core.session import Session



from multiprocessing import Process, Manager

def f(d):
    time.sleep(60)
    d[1] += '1'
    d['2'] += 2

if __name__ == '__main__':
    manager = Manager()
    config_path= os.path.join(os.getcwd(), 'config.txt')
    config = readConfig(config_path)
    hostname = getHostName()
    setup = config[hostname]       
    server = Server(setup)
    server.statusDict['currentSessions'] = manager.dict()
    d = {1: '1', '2':2}
#    obj1 = Session(server, 'abc', 3)
#    obj2 = Session(server, 'cde',10)
    p1 = Process(target=f, args=(d,))
#    p2 = Process(target=obj2.main)
#    p1.start()
#    p2.start()
    
#    for i in range(10):
#        time.sleep(1)
#        print(server.currentSessions)
    
#    p1.join()
#    p2.join()

#    print d