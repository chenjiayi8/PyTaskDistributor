#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 10:25:45 2021

@author: frank
"""


if __name__ == '__main__':
    import os
    from PyTaskDistributor.util.config import readConfig, getHostName
    from PyTaskDistributor.util.monitor import *
    from PyTaskDistributor.core.master import Master
    import math
    config_path= os.path.join(os.getcwd(), 'config.txt')
    config = readConfig(config_path)
    hostname = getHostName()
    setup = config[hostname]        
    master = Master(setup)
    master.updateServerList(10000)
    monitor = Monitor(master)
    monitor.printProgress()

        
