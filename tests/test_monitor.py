#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 10:25:45 2021

@author: frank
"""

if __name__ == '__main__':
    from PyTaskDistributor.util.config import read_config, get_host_name
    from PyTaskDistributor.util.monitor import *
    from PyTaskDistributor.core.master import Master

    config_path = os.path.join(os.getcwd(), 'config.txt')
    config = read_config(config_path)
    hostname = get_host_name()
    setup = config[hostname]
    master = Master(setup)
    master.update_server_list(10000)
    monitor = Monitor(master)
    monitor.print_progress()
