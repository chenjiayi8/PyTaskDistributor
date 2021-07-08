#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:05:04 2020

@author: frank
"""

import math
import os
import random
import shutil
import sys
import traceback
from datetime import datetime
from itertools import product as prod
from os.path import join as p_join

from dateutil import parser
import numpy as np
import pandas as pd

from PyTaskDistributor.util.extract import extract_between
from PyTaskDistributor.util.json import (read_json_to_df, read_json_to_dict, write_json_from_df)
from PyTaskDistributor.util.monitor import Monitor
from PyTaskDistributor.util.others import sleep_mins, update_xlsx_file, get_uuid


class Master:
    def __init__(self, setup, fast_mode=False):
        self.setup = setup
        self.default_folder = os.getcwd()
        self.fast_mode = fast_mode
        self.server_folder = p_join(self.default_folder, 'Servers')
        self.main_folder = self.setup['order']
        self.new_task_folder = p_join(self.main_folder, 'NewTasks')
        self.finished_task_folder = p_join(self.main_folder, 'FinishedTasks')
        self.task_file_path = p_join(self.main_folder, 'TaskList.xlsx')
        self.last_modified_time = ''
        self.monitor = Monitor(self)
        self.server_list = []
        self.msgs = []
        pass

    def main(self):
        num_min = random.randint(3, 5)
        try:
            if self.last_modified_time != self.get_file_last_modified_time():
                self.generate_tasks()
                self.generate_manual_tasks()
            self.update_server_list()
            self.update_task_status()
            self.remove_finished_task()
            self.workload_balance()
            self.print_progress(num_min)
            self.print_msgs()
            sleep_mins(num_min)
            need_assistance = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print("Need assistance for unexpected error:\n {}".format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            need_assistance = True
        return need_assistance

    def print_progress(self, num_min=5):
        self.monitor.print_progress(num_min)

    def print_msgs(self):
        for msg in self.msgs:
            print(msg)
        self.msgs.clear()

    def get_file_last_modified_time(self, file_path=None):
        if file_path is None:
            file_path = self.task_file_path
        modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        return modified_time

    def get_task_list(self, folder=None, ending=".json"):
        if folder is None:
            folder = self.new_task_folder
        task_list = [f for f in os.listdir(folder) if f.endswith(ending)]

        def not_clean_or_purge(task):  # skip these task for another server
            if task.lower().endswith('_clean.json'):
                return False
            if task.lower().endswith('_purge.json'):
                return False
            return True

        task_list = list(filter(not_clean_or_purge, task_list))
        return task_list

    def update_server_list(self, timeout_mins=30):
        server_list = [f for f in os.listdir(self.server_folder) if f.endswith(".json")]
        self.server_list.clear()
        for s in server_list:
            try:
                s_path = p_join(self.server_folder, s)
                if 'sync-conflict' in s:
                    os.unlink(s_path)
                else:
                    s_json = read_json_to_dict(s_path)
                    s_time = parser.parse(s_json['updated_time'])
                    diff_min = (datetime.now() - s_time).seconds / 60
                    if diff_min < timeout_mins:  # only use if updated within N mins
                        self.server_list.append(s_json)
            except:
                print("Failed to read status of {}".format(s))
                pass

    def workload_balance(self):
        task_list = self.get_task_list()
        for task in task_list:
            if 'sync-conflict' in task:  # conflict file from Syncthing
                task_path = p_join(self.new_task_folder, task)
                os.unlink(task_path)
        if len(task_list) == 0:
            return
        task = random.choice(task_list)  # Only balance one task per time
        task_path = p_join(self.new_task_folder, task)
        df = read_json_to_df(task_path)
        df = df.sort_values('Num')
        temp = list(zip(['Task'] * len(df), df['Num'].apply(str), df['UUID']))
        index = ['-'.join(t) for t in temp]
        df.index = index
        df = df.fillna('')
        fasted_flag = self.fast_mode
        # override fastmode to True for small task
        if len(df) <= len(self.server_list) * 4:
            fasted_flag = True
        num_server = len(self.server_list)
        for i in range(num_server):
            server = self.server_list[i]
            # check if assigned sessions are running
            skip_flag = False
            num_target = 0
            msg_cause = ''
            df_temp = df[df['HostName'] == '']
            # No more sessions
            if len(df_temp) == 0:
                msg_cause = 'All sessions are assigned\n'
                msg = "Assign 0 new sessions of {} for Server {} because: {}".format(task, server['name'], msg_cause)
                self.msgs.append(msg)
                continue

            # assigned sessions but not running
            if len(server['currentSessions']) > server['num_running']:
                skip_flag = True
                msg_cause += 'Assigned sessions are not running\n'
            df_assigned = df[(df['HostName'] == server['name']) & (df['Finished'] != 1)]

            # assigned sessions but not received
            for idx in df_assigned.index:
                if idx not in server['currentSessions']:
                    finished_sessions = server['finishedSessions'].keys()
                    idx_in_finished_sessions = [idx in s for s in finished_sessions]
                    if not any(idx_in_finished_sessions):
                        if not skip_flag:
                            msg_cause += '\n'
                        skip_flag = True
                        msg_cause += 'Assigned session {} are not received\n'.format(idx)

            if not skip_flag:
                if int(server['num_running']) == 0:
                    num_target = 4
                else:
                    cpu_available = server['CPU_max'] - server['CPU_total']
                    cpu_per_task = server['CPU_matlab'] / server['num_running']
                    num_cpu = math.floor(cpu_available / cpu_per_task)
                    mem_available = server['MEM_max'] - server['MEM_total']
                    mem_per_task = server['MEM_matlab'] / server['num_running']
                    num_mem = math.floor(mem_available / mem_per_task)
                    num_target = min([num_cpu, num_mem])
                    if num_target > 4:  # Max add 4 per cycle
                        num_target = 4
                    if num_target < 0:
                        num_target = 0
                # CPU/MEM limit
                if num_target == 0:
                    skip_flag = True
                    msg_cause += 'reaching CPU/MEM limit'

            # Do not assign new session
            if skip_flag:
                msg = "Assign 0 new sessions of {} for Server {} because: {}" \
                    .format(task, server['name'], msg_cause)
            else:  # assign new session
                if fasted_flag:
                    num_target = math.ceil(len(df_temp) / len(self.server_list))
                    if i == num_server - 1:  # Last Server get all sessions
                        num_target = len(df_temp)
                index = list(df_temp.index)
                random.shuffle(index)
                if len(index) > num_target:
                    index = index[:num_target]
                else:
                    num_target = len(index)
                for ii in range(len(index)):
                    df.loc[index[ii], 'HostName'] = server['name']
                msg = "Assign {} new sessions of {} for Server {}".format(num_target, task, server['name'])
            # record msg
            self.msgs.append(msg)
        write_json_from_df(task_path, df)

    def get_finished_sessions(self):
        finished_sessions = {}
        for server in self.server_list:
            temp_dict = server['finishedSessions']
            for _, v in temp_dict.items():
                v['HostName'] = server['name']
            finished_sessions.update(temp_dict)
        return finished_sessions

    @staticmethod
    def get_time_str(task):
        return extract_between(task, 'TaskList_', '.json')[0]

    @staticmethod
    def read_task(path):
        df = read_json_to_df(path)
        df = df.sort_values('Num')
        temp = list(zip(['Task'] * len(df), df['Num'].apply(str), df['UUID']))
        index = ['-'.join(t) for t in temp]
        df.index = index
        df = df.fillna('')
        return df

    @staticmethod
    def mark_task_df(task, df, finished_sessions):
        modified_flag = False
        for key in finished_sessions.keys():
            underline_positions = [i for i, ltr in enumerate(key) if ltr == '_']
            task_time_str = key[:underline_positions[1]]
            index = key[underline_positions[1] + 1:]
            if task_time_str in task and index in df.index:
                # only modify when necessary
                if df.loc[index, 'Finished'] != 1:
                    values = finished_sessions[key]
                    modified_flag = True
                    for k, v in values.items():
                        df.loc[index, k] = v
        return modified_flag

    def remove_finished_task(self):
        task_list = self.get_task_list(folder=self.finished_task_folder)
        if len(task_list) == 0:  # skip if no new finished task
            return
        finished_sessions = self.get_finished_sessions()
        for task in task_list:
            path = p_join(self.finished_task_folder, task)
            path_xlsx = p_join(self.main_folder, 'Output', task[:-5] + '.xlsx')
            df = self.read_task(path)
            modified_flag = self.mark_task_df(task, df, finished_sessions)
            if modified_flag:
                write_json_from_df(path, df)
                update_xlsx_file(path_xlsx, df)
            if all(df['Finished'] == 1):  # rename to json.bak
                path_done = p_join(self.finished_task_folder, task + '.bak')
                shutil.move(path, path_done)

    def update_task_status(self):
        task_list = self.get_task_list()
        finished_sessions = self.get_finished_sessions()
        for task in task_list:
            path = p_join(self.new_task_folder, task)
            path_xlsx = p_join(self.main_folder, 'Output', task[:-5] + '.xlsx')
            df = self.read_task(path)
            modified_flag = self.mark_task_df(task, df, finished_sessions)
            if modified_flag:
                write_json_from_df(path, df)
                update_xlsx_file(path_xlsx, df)
            if all(df['Finished'] == 1):  # move to finish folder
                path_done = p_join(self.finished_task_folder, task)
                shutil.move(path, path_done)

    def exist_task(self, json_name):
        file_list = []
        folder_list = [self.finished_task_folder, self.new_task_folder]
        endings = ['json', 'bak', 'delete']
        for folder in folder_list:
            for ending in endings:
                file_list += self.get_task_list(folder=folder, ending=ending)

        return any([json_name in file for file in file_list])

    def generate_manual_tasks(self):
        manual_xlsx = p_join(self.main_folder, 'TaskList_manual.xlsx')
        if os.path.isfile(manual_xlsx):
            last_modified_time = self.get_file_last_modified_time(manual_xlsx)
            last_modified_time_str = datetime.strftime(last_modified_time, "%Y%m%d_%H%M%S")
            json_name = 'TaskList_' + last_modified_time_str + '.json'
            if self.exist_task(json_name):  # already created
                return
            new_json_name = p_join(self.new_task_folder, json_name)
            df = pd.read_excel(manual_xlsx)
            df = df.fillna('')
            df.loc[:, 'UUID'] = df.loc[:, 'UUID'].apply(get_uuid)
            write_json_from_df(new_json_name, df)

    def generate_tasks(self):
        self.last_modified_time = self.get_file_last_modified_time()
        last_modified_time_str = datetime.strftime(self.last_modified_time, "%Y%m%d_%H%M%S")
        json_name = 'TaskList_' + last_modified_time_str + '.json'
        if self.exist_task(json_name):  # already created
            return
        new_json_name = p_join(self.new_task_folder, json_name)
        modified_time = os.path.getmtime(self.task_file_path)
        parameter_table = pd.read_excel(self.task_file_path, sheet_name='ParameterRange')
        parameter_ids = list(range(0, 5)) + list(range(7, 15 + 5))
        parameter_names = list(parameter_table.columns)
        parameter_names_target = [parameter_names[i] for i in parameter_ids]
        num_total_task = 1
        for name in parameter_names_target:
            vector = parameter_table.loc[:, name]
            num_non_nan = list(np.isnan(vector)).count(False)
            num_total_task *= num_non_nan

        columns = ['Num'] + parameter_names_target
        vector_list = []
        for name in parameter_names_target:
            vector = parameter_table.loc[:, name]
            non_nan_idx = np.isnan(vector)
            non_nan_idx = [not i for i in non_nan_idx]
            vector = [float(vector[i]) for i in range(len(vector))
                      if non_nan_idx[i]]
            vector_list.append(vector)

        combinations = list(prod(*vector_list))
        combinations = [[i + 1] + list(combinations[i]) for i in range(len(combinations))]
        task_table = pd.DataFrame(data=combinations, columns=columns)
        task_table_old = pd.read_excel(self.task_file_path, sheet_name='Sheet1')
        columns_old = list(task_table_old.columns)
        for c in columns_old:
            if c not in task_table.columns:
                task_table[c] = ''
        task_table.loc[:, 'UUID'] = task_table.loc[:, 'UUID'].apply(get_uuid)
        update_xlsx_file(self.task_file_path, task_table)
        task_table = pd.read_excel(self.task_file_path, sheet_name='Sheet1')
        new_xlsx_name = p_join(self.main_folder, 'Output', 'TaskList_' + last_modified_time_str + '.xlsx')
        write_json_from_df(new_json_name, task_table)
        shutil.copyfile(self.task_file_path, new_xlsx_name)
        os.utime(self.task_file_path, (modified_time, modified_time))
        os.utime(new_json_name, (modified_time, modified_time))
        os.utime(new_xlsx_name, (modified_time, modified_time))


if __name__ == '__main__':
    pass
