#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 11:20:47 2021

@author: frank
"""

import os
import time
from multiprocessing import Manager, Process

from PyTaskDistributor.core.server import Server
from PyTaskDistributor.util.config import get_host_name, read_config


def f(_d):
    time.sleep(60)
    _d[1] += '1'
    _d['2'] += 2


if __name__ == '__main__':
    manager = Manager()
    config_path = os.path.join(os.getcwd(), 'config.txt')
    config = read_config(config_path)
    hostname = get_host_name()
    setup = config[hostname]
    server = Server(setup)
    server.status_dict['current_sessions'] = manager.dict()
    d = {1: '1', '2': 2}
    #    obj1 = Session(server, 'abc', 3)
    #    obj2 = Session(server, 'cde',10)
    p1 = Process(target=f, args=(d,))
#    p2 = Process(target=obj2.main)
#    p1.start()
#    p2.start()

#    for i in range(10):
#        time.sleep(1)
#        print(server.current_sessions)

#    p1.join()
#    p2.join()

#    print d
