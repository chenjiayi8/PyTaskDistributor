#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 00:38:06 2021

@author: frank
"""

import os
from datetime import datetime

from dateutil import parser
import pandas as pd
import tabulate as tb

from PyTaskDistributor.util.json import read_json_to_df
from PyTaskDistributor.util.others import print_table


def parse_time(t):
    if type(t) == str:
        t = parser.isoparse(t)
    return datetime.strftime(t, "%d/%m/%Y %H:%M:%S")


def clear_console():
    command = 'clear'
    if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
        command = 'cls'
    os.system(command)


def get_time_str(task):
    mark_location = [i for i, ltr in enumerate(task) if ltr == '_']
    dot_location = [i for i, ltr in enumerate(task) if ltr == '.']
    time_str = task[mark_location[0] + 1:dot_location[-1]]
    return time_str


def get_duplicated_items(lst1, lst2):
    return set(lst1) & set(lst2)


def is_server_state(_input):
    return any(x in _input for x in ['CPU', 'MEM', 'DISK'])


class Monitor:
    def __init__(self, master):
        self.master = master
        self.columns_server = ['name', 'CPU_max', 'MEM_max', 'CPU_total',
                               'MEM_total', 'DISK_total', 'CPU_matlab',
                               'MEM_matlab', 'num_assigned',
                               'num_running', 'num_finished',  'updated_time']
        self.columns_task = ['name', 'num_task', 'num_assigned', 'num_running',
                             'num_finished', 'updated_time']
        pass

    def print_progress(self, num_mins=5):
        clear_console()
        print("Updated on {} and will reload in {} mins".format(
                parse_time(datetime.now()), num_mins))
        self.print_task_progress()
        return self.print_server_progress()

    def print_task_progress(self):
        if self.master.fast_mode:
            print("Task (Fast mode):")
        else:
            print("Task:")
        df_tasks = pd.DataFrame(columns=self.columns_task)
        task_list = self.master.get_task_list()
        assigned_sessions = []
        finished_sessions = []
        current_sessions = []
        for server in self.master.server_list:
            assigned_sessions += server['assigned_sessions']
            finished_sessions += server['finished_sessions'].keys()
            current_sessions += server['current_sessions']
        for task in task_list:
            time_str = get_time_str(task)
            task_path = os.path.join(self.master.new_task_folder, task)
            df = read_json_to_df(task_path)
            df = df.sort_values('Num')
            df['UUID'] = df['UUID'].apply(str)
            temp = list(zip(['Task'] * len(df), df['Num'].apply(str),
                            df['UUID']))
            index1 = ['-'.join(t) for t in temp]
            index2 = [time_str + '_' + '-'.join(t) for t in temp]
            num_task = len(df)
            num_assigned = len(get_duplicated_items(assigned_sessions, index1))
            num_finished = len(get_duplicated_items(finished_sessions, index2))
            num_running = len(get_duplicated_items(current_sessions, index1))
            updated_time = datetime.fromtimestamp(os.path.getmtime(task_path))
            updated_time_str = parse_time(updated_time)
            data = [task[:-5], num_task, num_assigned, num_running,
                    num_finished, updated_time_str]
            df_tasks = df_tasks.append(pd.DataFrame(data=[data],
                                                    columns=self.columns_task))
        print_table(df_tasks)

    def print_server_progress(self):
        print("Server:")
        df = pd.DataFrame(columns=self.columns_server)
        for server in self.master.server_list:
            data = [server[k] for k in self.columns_server]
            df = df.append(pd.DataFrame(data=[data],
                                        columns=self.columns_server))
        df['updated_time'] = df['updated_time'].apply(parse_time)
        df = df.fillna(0)
        columns_new = [a + '(%)' if is_server_state(a) else a
                       for a in df.columns]
        df.columns = columns_new
        df_target = df.loc[:, columns_new[1:]]
        col_head = [columns_new[0]] + df.loc[:, columns_new[0]].values.tolist()
        num_columns = len(df_target.columns)
        num_groups = 2
        num_interval = round(num_columns / num_groups)
        parts = []
        sep_ids = list(range(0, num_columns, num_interval))
        for i in range(num_groups):
            if i != num_groups - 1:
                target_ids = list(range(sep_ids[i], sep_ids[i + 1]))
            else:
                target_ids = list(range(sep_ids[-1], num_columns))
            df_temp = df_target.iloc[:, target_ids]
            part = [list(df_temp.columns)] + df_temp.values.tolist()
            if len(df_temp.columns) < num_interval:
                for p in part:
                    p.insert(0, '')
            for j in range(len(part)):
                part[j] = [col_head[j]] + part[j]
            parts += part
        table = tb.tabulate(parts, tablefmt="grid")
        print(table)


if __name__ == '__main__':
    pass
