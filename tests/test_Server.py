#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:05 2021

@author: frank
"""

from PyTaskDistributor.util.config import readConfig#, getHostName
from PyTaskDistributor.core.server import Server


config_path=r'/home/frank/LinuxWorkFolder/TranReNu/PyTaskDistributor/config.txt'

config = readConfig(config_path)
#hostname = getHostName()

hostname = 'cmmb01'
setup = config[hostname]
setup['order'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
setup['delivery'] = '/home/frank/LinuxWorkFolder/TranReNu/MC3DVersion3.3_Git/'
setup['factory'] = '/home/frank/LocalWorkFolder/TranReNu/MC3DVersion3.3_Git/'        
obj = Server(setup)

