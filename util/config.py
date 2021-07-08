#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:59:40 2020

@author: frank
"""

from PyTaskDistributor.util.extract import extract_between
import platform


def get_host_name():
    return platform.uname()[1].lower()


def read_config(config_path):
    with open(config_path, 'r') as f:
        config_lines = f.readlines()
        config_lines = [line.replace('\n', '') for line in config_lines]
        left_mark_locations = [i for i in range(len(config_lines)) if config_lines[i].find('[') == 0]
        config_list = []
        for i in range(len(left_mark_locations)-1):
            start = left_mark_locations[i]
            end = left_mark_locations[i+1]
            config_list.append(config_lines[start:end])
        else:
            start = left_mark_locations[i+1]
            end = len(config_lines)
            config_list.append(config_lines[start:end])
        config = {}
        for group in config_list:
            temp_dict = {}
            for i in range(len(group)):
                item = group[i]
                if i == 0:
                    temp_dict['role'] = extract_between(item, '[', ']')[0]
                else:
                    item_list = item.split(':')
                    temp_dict[item_list[0]] = item_list[1]
            config[temp_dict['hostname']] = temp_dict
        return config


if __name__ == "__main__":
    pass
