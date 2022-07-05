#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 13:21:08 2021

@author: frank
"""

import math
import os
import random
import shutil
import sys
import traceback
from collections import OrderedDict
# import time
from datetime import datetime
# from distutils.dir_util import copy_tree
from glob import glob
from multiprocessing import Manager
from os.path import isdir, isfile, join as p_join

# import matlab.engine
import dirsync
import numpy as np
import psutil

from PyTaskDistributor.core.session import Session
from PyTaskDistributor.util.extract import extract_between
from PyTaskDistributor.util.json import (
    read_json_to_df, read_json_to_dict, write_json_from_dict)
from PyTaskDistributor.util.others import (
    get_file_suffix, get_latest_file_in_folder, get_process_cpu,
    get_process_mem, get_process_list, make_dirs, get_num_processor,
    sleep_mins
    )


class Server:
    def __init__(self, setup, debug=False):
        self.print("Server {} started".format(setup['hostname']))
        self.setup = setup
        self.debug = debug
        self.CPU_max = float(setup['CPU_max']) * 100
        self.MEM_max = float(setup['MEM_max']) * 100
        self.default_folder = os.getcwd()
        self.server_folder = p_join(self.default_folder, 'Servers')
        self.host_name = self.setup['hostname']
        self.user_name = os.popen('whoami').readline().split('\n')[0]
        if not isdir(self.server_folder):
            os.makedirs(self.server_folder)
        self.status_file = p_join(self.server_folder,
                                  self.host_name + '.json')
        if isfile(self.status_file):
            self.status_dict = read_json_to_dict(self.status_file)
        else:
            self.status_dict = OrderedDict()
        self.status_dict_clean_counter = 0
        self.main_folder = self.setup['order']
        self.factory_folder = self.setup['factory']
        self.delivery_folder = self.setup['delivery']
        self.new_task_folder = p_join(self.main_folder, 'NewTasks')
        self.finished_task_folder = p_join(self.main_folder, 'FinishedTasks')
        self.excluded_folder = ['Output', 'NewTasks', 'FinishedTasks', '.git']
        self.delivery_folder_path = ''
        self.mat_folder_path = ''
        self.task_time_str = ''
        self.manager = Manager()
        self.current_sessions = self.manager.dict()
        self.sessions_dict = {}
        self.status_names = ['CPU_total', 'MEM_total',
                             'CPU_matlab', 'MEM_matlab']
        self.status_record = np.zeros(len(self.status_names) + 1, dtype=float)
        self.initialise()

    def initialise(self):
        self.kill_residual_sessions()
        make_dirs(self.factory_folder)
        make_dirs(self.delivery_folder)
        if isfile(self.status_file):
            # update config
            self.status_dict['name'] = self.host_name
            self.status_dict['user'] = self.user_name
            self.status_dict['CPU_max'] = self.CPU_max
            self.status_dict['MEM_max'] = self.MEM_max
        else:
            self.reset_status_dict()
        self.record_status()
        self.write_server_status()

    def reset_status_dict(self):
        self.status_dict = OrderedDict()
        self.status_dict['name'] = self.host_name
        self.status_dict['user'] = self.user_name
        self.status_dict['CPU_max'] = self.CPU_max
        self.status_dict['MEM_max'] = self.MEM_max
        self.status_dict['msg'] = []
        self.status_dict['assigned_sessions'] = []
        self.status_dict['current_sessions'] = OrderedDict()
        self.status_dict['finished_sessions'] = OrderedDict()

    @staticmethod
    def print(msg):
        time_prefix = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')
        time_prefix = time_prefix[:-3] + '$ '
        msg = time_prefix + msg + '\n'
        print(msg)

    def kill_residual_sessions(self):
        # kill existing processes
        for _, v in self.status_dict['current_sessions'].items():
            os.system("kill -9 {}".format(v['pid']))

        self.status_dict['current_sessions'].clear()

    def kill_all_sessions(self):
        # kill all matlab processes
        os.system("kill -9 $(pgrep -f 'MATLAB -mvmInputPipe')")

    def main(self):
        num_min = random.randint(2, 4)
        interval_seconds = 30
        interval_mins = interval_seconds / 60
        num_interval = round(num_min * 60 / interval_seconds)
        try:
            self.on_interval()
            self.print("Sleeping for {} mins".format(num_min))
            for i in range(num_interval):
                self.record_status()
                sleep_mins(interval_mins)
                self.cleanSyncConflictState()
            self.write_server_status()
            need_assistance = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.print("Need assistance for unexpected error:\n {}"
                       .format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            need_assistance = True
        return need_assistance

    def on_interval(self):
        self.manual_remove_task()
        self.remove_finished_task()
        task_list = self.get_task_list()
        clean_task = self.host_name + '_clean.json'
        purge_task = self.host_name + '_purge.json'

        # reset everything in factory
        if purge_task in task_list:
            self.print("Resetting factory")
            self.kill_all_sessions()
            self.purge_factory()
            self.kill_residual_sessions()
            self.reset_status_dict()
            self.update_server_status()
            os.unlink(p_join(self.new_task_folder, purge_task))
            task_list.remove(purge_task)
            if clean_task in task_list:  # no need to clean anymore
                os.unlink(p_join(self.new_task_folder, clean_task))
                task_list.remove(clean_task)

        # clean previous task results
        if clean_task in task_list:
            self.print("Cleaning unfinished tasks")
            self.kill_all_sessions()
            self.clean_unfinished_tasks()
            self.prepare_factory()
            self.kill_residual_sessions()
            self.reset_status_dict()
            self.update_server_status()
            os.unlink(p_join(self.new_task_folder, clean_task))
            task_list.remove(clean_task)

        task_list = list(filter(self.validedTask, task_list))

        if len(task_list) > 0:
            self.status_dict_cleaner(reset=True)
            # work on one task per cycle
            task = random.choice(task_list)
            # announce the start of simulation
            self.print("Working on {}".format(task))
            self.update_folder_paths(task)  # paths for output
            self.on_start_task()
            df = self.get_task_table(task, unfinished=True)  # check new task
            if df is not None:
                df = self.remove_finished_inputs(df)
                sessions = self.create_sessions(df, task)
                sessions = self.workload_balance(sessions)
                self.run_sessions(sessions)
        else:  # clean if no task at all
            self.status_dict_cleaner(reset=False)

        # wait for the starting of simulation
        sleep_mins(1)
        self.update_sessions_status()
        self.deal_with_zombie_sessions()

    def status_dict_cleaner(self, reset=False):
        if reset:  # reset counter
            self.status_dict_clean_counter = 0
        else:
            self.status_dict_clean_counter += 1

        # no task for N cycles, clear status_dict
        if self.status_dict_clean_counter > 10:
            self.status_dict_clean_counter = 0
            self.status_dict['assigned_sessions'].clear()
            self.status_dict['current_sessions'].clear()
            self.status_dict['finished_sessions'].clear()

    def on_start_task(self):
        if int(self.status_dict['num_running']) == 0:
            self.prepare_factory()
        sessions = self.get_finished_sessions()
        if sessions:
            self.mark_finished_session(sessions)
            self.write_server_status()
        # remove finished sessions from current sessions
        self.remove_finished_sessions()

    def write_server_status(self):
        # calculate the average CPU/MEM percents and write
        if self.status_record[-1] > 0:
            averages = self.status_record[:-1] / self.status_record[-1]
            averages = averages.round(2)
            for i in range(len(self.status_names)):
                self.status_dict[self.status_names[i]] = averages[i]
        if not self.debug:  # do not write status during debugging
            write_json_from_dict(self.status_file, self.status_dict)
        self.status_record = np.zeros(len(self.status_names) + 1, dtype=float)

    def record_status(self):
        self.update_server_status()
        state = [float(self.status_dict[s]) for s in self.status_names]
        self.status_record[:-1] += state
        self.status_record[-1] += 1
        pass

    def get_disk_usage_percent(self):
        (total, used, free) = shutil.disk_usage(self.factory_folder)
        return round(100 * used / total, 2)

    def get_processes(self):
        df = get_process_list()
        df_user = df[df['User'] == self.user_name]
        # only count matlab process opened by Python
        df_matlab = df_user[df_user['Command'].apply(
            lambda x: 'matlab -mvminputpipe' in x.lower())]
        return df, df_matlab

    def deal_with_zombie_sessions(self):
        zombie_sessions = []
        for k, v in self.status_dict['current_sessions'].items():
            if v['zombie'] > 20:
                zombie_sessions.append(k)

        for k in zombie_sessions:
            if k in self.sessions_dict:
                # delete session because of being zombie
                self.sessions_dict[k].clean_workspace('being zombie')
                del self.sessions_dict[k]
            if k in self.current_sessions:
                del self.current_sessions[k]
            if k in self.status_dict['current_sessions']:
                del self.status_dict['current_sessions'][k]

    def update_server_status(self):
        df, _ = self.get_processes()
        self.status_dict['CPU_total'] = round(psutil.cpu_percent(interval=1),
                                              2)
        self.status_dict['MEM_total'] = round(sum(df['Mem']), 2)
        self.status_dict['DISK_total'] = self.get_disk_usage_percent()
        self.status_dict['num_assigned'] = self.get_num_assigned()
        self.status_dict['num_finished'] = \
            int(len(self.status_dict['finished_sessions']))

        num_running = 0
        CPU_matlab = 0.0
        MEM_matlab = 0.0
        for k in self.status_dict['current_sessions']:
            s = self.status_dict['current_sessions'][k]
            s['cpu'] = round(get_process_cpu(s['pid']), 2)
            s['mem'] = round(get_process_mem(s['pid']), 2)
            s['zombie'] = s['zombie'] + 1 if s['cpu'] < 10.0 else 0
            num_running += 1
            CPU_matlab += s['cpu']
            MEM_matlab += s['mem']

        self.status_dict['num_running'] = num_running
        self.status_dict['CPU_matlab'] = \
            round(CPU_matlab/get_num_processor(), 2)
        self.status_dict['MEM_matlab'] = MEM_matlab
        self.status_dict['updated_time'] = datetime.now().isoformat()

    @staticmethod
    def validedTask(task):  # skip these task
        task_lower = task.lower()
        if task_lower.endswith('clean.json'):
            return False
        if task_lower.endswith('purge.json'):
            return False
        if 'sync-conflict' in task_lower:
            return False
        return True

    def get_num_assigned(self):
        num_assigned = 0
        task_list = self.get_task_list()
        task_list = list(filter(self.validedTask, task_list))
        for task in task_list:
            path = p_join(self.new_task_folder, task)
            df = read_json_to_df(path)
            df_assigned = df[df['HostName'] == self.host_name]
            num_assigned += len(df_assigned)

        return num_assigned

    def mark_finished_session(self, sessions):
        if len(sessions) > 0:
            target_sessions = []
            target_mat_paths = []
            sessions_temp = []
            for session in sessions:
                key = self.task_time_str + '_' + session
                if key not in self.status_dict['finished_sessions']:
                    mat_folder = p_join(self.mat_folder_path, session, 'data')
                    mat_path = get_latest_file_in_folder(mat_folder, '.mat')
                    if mat_path:
                        target_mat_paths.append(mat_path)
                        target_sessions.append(session)
                    else:
                        log_file = p_join(self.mat_folder_path,
                                          session, session + '.txt')
                        with open(log_file, 'rt') as f:
                            lines = f.read().splitlines()
                            lines = lines[1:]
                            msg = {'Finished': 1, 'err_msg': '|'.join(lines)}
                            self.status_dict['finished_sessions'][key] = msg

            for i in range(len(target_mat_paths)):
                obj = Session(self, target_sessions[i], target_mat_paths[i],
                              self.task_time_str)
                json_path = target_mat_paths[i][:-4] + '.json'
                if isfile(json_path):
                    output = obj.read_output(json_file=json_path)
                    if output is not None:
                        key = self.task_time_str + '_' + target_sessions[i]
                        self.status_dict['finished_sessions'][key] = output
                    else:
                        obj.main(target='mark_finished_session')
                        sessions_temp.append(obj)
                else:
                    obj.main(target='mark_finished_session')
                    sessions_temp.append(obj)

            for session in sessions_temp:
                session.process.join()

    def get_finished_sessions(self):
        if isdir(self.mat_folder_path):
            sessions = os.listdir(self.mat_folder_path)
            finished_sessions = []
            for session in sessions:
                if not isfile(p_join(self.mat_folder_path, session,
                                     'data', 'final.mat')):
                    continue
                key = self.task_time_str + '_' + session
                if key not in self.status_dict['finished_sessions']:
                    finished_sessions.append(session)
            return finished_sessions
        else:
            return None

    def get_unfinished_sessions(self):
        output_folder = p_join(self.factory_folder, 'Output')
        make_dirs(output_folder)
        unfinished_ss = os.listdir(output_folder)
        unfinished_ss = [s for s in unfinished_ss if s.startswith('Task-')]
        return unfinished_ss, output_folder

    def get_task_progresses(self, df):
        task_progresses = {}
        for index in df.index:
            task_progresses[index] = {'finished': False,  'latest': None,
                                      'factory': None, 'delivery': None}
        output_folders = {'factory': p_join(self.factory_folder, 'Output'),
                          'delivery': self.mat_folder_path}
        for name, folder in output_folders.items():
            make_dirs(folder)
            sessions = os.listdir(folder)
            sessions = [s for s in sessions if s in df.index]
            for s in sessions:
                if task_progresses[s]['finished']:
                    continue
                data_folder = p_join(folder, s, 'data')
                if isfile(p_join(data_folder, 'final.mat')):
                    task_progresses[s]['finished'] = True
                    task_progresses[s]['latest'] = p_join(data_folder,
                                                          'final.mat')
                    task_progresses[s][name] = p_join(folder, s)
                else:
                    latest = get_latest_file_in_folder(data_folder, '.mat')
                    err_json = get_latest_file_in_folder(data_folder,
                                                         '-err.json')
                    if latest is None:
                        continue
                    if err_json is not None:
                        err_msg = read_json_to_dict(err_json)
                        self.status_dict['msg'].append(
                                '{} has err msg:\n{}'.format(
                                        s, err_msg['err_msg']))
                    if task_progresses[s]['latest'] is None:
                        task_progresses[s]['latest'] = latest
                        task_progresses[s][name] = p_join(folder, s)
                    else:
                        if os.path.getmtime(latest) > \
                           os.path.getmtime(task_progresses[s]['latest']):
                            task_progresses[s]['latest'] = latest
                            task_progresses[s][name] = p_join(folder, s)

        return task_progresses

    def remove_finished_inputs(self, df):
        if len(df) == 0:
            return df
        make_dirs(self.mat_folder_path)  # create if doest not exist
        finished_sessions = self.get_finished_sessions()
        unwanted_inputs = []
        for session in finished_sessions:
            if session in df.index:
                unwanted_inputs.append(session)
        df2 = df.drop(unwanted_inputs)
        return df2

    def is_running(self, session):
        if session not in self.sessions_dict:
            return False
        else:
            s = self.sessions_dict[session]
            if not s.process.is_alive():
                return False
            else:
                return True

    def create_sessions(self, df, task):
        task_progress = self.get_task_progresses(df)
        columns = list(df.columns)
        input_columns = []
        for c in columns:
            if c.lower() != 'hostname':
                input_columns.append(c)
            else:
                break

        def get_input(_input):
            _input = list(_input)
            _input = [float(_input[i]) if i < len(_input) - 1
                      else _input[i] for i in range(len(_input))]
            return _input

        sessions = {}
        for i in range(len(df)):
            session = df.index[i]
            if not task_progress[session]['finished']:  # not finished
                if not self.is_running(session):  # not running now
                    mat_path = task_progress[session]['latest']
                    if mat_path is not None:  # Progress is saved
                        sessions[session] = Session(self, session, mat_path)
                    else:  # Nothing is saved or not started yet
                        _input = get_input(df.loc[session, input_columns])
                        sessions[session] = Session(self, session, _input)
            else:
                # finished but not delivered
                if task_progress[session]['factory'] is not None:
                    time_str = self.get_time_str(task)
                    s = Session(self, session, None, time_str)
                    s.post_process()

        return sessions

    def deal_with_failed_session(self, name):
        self.status_dict['msg'].append(
            '{} failed on {}'.format(name, datetime.now()))
        if name in self.status_dict['current_sessions']:
            del self.status_dict['current_sessions'][name]
        if name in self.current_sessions:
            del self.current_sessions[name]
        if name in self.sessions_dict:
            del self.sessions_dict[name]

    def none(self, msg):
        self.print(msg)
        return None

    def workload_balance(self, sessions):
        d = self.status_dict
        if d['CPU_total'] > self.CPU_max:
            return self.none("CPU overflow")
        if d['MEM_total'] > self.MEM_max:
            return self.none("MEM overflow")
        if len(sessions) == 0:
            return self.none("Zero task")
        num_target = 2
        num_default = 2
        num_current = len(self.current_sessions)
        if num_current == 0:
            num_target = 4

        if d['num_running'] > 0:
            try:
                cpu_available = d['CPU_max'] - d['CPU_total']
                cpu_per_task = d['CPU_matlab'] / d['num_running']
                num_cpu = math.floor(cpu_available / cpu_per_task)
                mem_available = d['MEM_max'] - d['MEM_total']
                mem_per_task = d['MEM_matlab'] / d['num_running']
                num_mem = math.floor(mem_available / mem_per_task)
                num_target = min([num_cpu, num_mem])
                if num_target > 4:  # Max add 4 per cycle
                    num_target = 4
                if num_target < 0:
                    num_target = 0
            except Exception:
                num_target = num_default

        self.print("Add maximum {} sessions".format(num_target))
        if num_target == 0:
            return self.none("Zero target")
        if len(sessions) > num_target:
            is_unfinished_list = [[s[1].is_unfinished, s[0]]
                                  for s in sessions.items()]
            random.shuffle(is_unfinished_list)
            is_unfinished_list = sorted(is_unfinished_list, reverse=True)
            sessions_new = {}
            for i in range(num_target):
                sessions_new[is_unfinished_list[i][1]] = \
                    sessions[is_unfinished_list[i][1]]

            return sessions_new
        else:
            return sessions

    def check_sessions(self):
        for k, s in self.sessions_dict.items():
            if not s.process.is_alive():
                self.print("Process for {} is killed".format(k))

    def run_sessions(self, sessions):
        if sessions is not None:
            # record pid first
            valided_sessions = []
            for k, s in sessions.items():
                exitcode = s.create_matlab_eng()
                if exitcode == 0:
                    valided_sessions.append(s)
                    self.sessions_dict[k] = s
                    self.status_dict['current_sessions'][k] = \
                        {'pid': s.pid, 'cpu': 0.0, 'mem': 0.0,
                         'zombie': 0}

            # run the session
            for s in valided_sessions:
                s.main()

            # go back to default_folder
            self.print(
                "{} sessions are running from this cycle"
                .format(len(sessions)))
            os.chdir(self.default_folder)

    def get_finished_session_key(self, session):
        task_list = self.get_task_list()
        task_list = list(filter(self.validedTask, task_list))
        for task in task_list:
            path = p_join(self.new_task_folder, task)
            df = read_json_to_df(path)
            df = df.sort_values('Num')
            df['UUID'] = df['UUID'].apply(str)
            temp = list(
                zip(['Task'] * len(df), df['Num'].apply(str), df['UUID']))
            index = ['-'.join(t) for t in temp]
            if session in index:
                time_str = self.get_time_str(task)
                key = time_str + '_' + session
                return key
        return None

    def remove_finished_sessions(self):
        sessions = list(self.status_dict['finished_sessions'].keys())
        for session in sessions:
            underline_positions = [
                i for i, ltr in enumerate(session) if ltr == '_']
            key = session[underline_positions[1] + 1:]
            if key in self.status_dict['current_sessions']:
                del self.status_dict['current_sessions'][key]
            if key in self.status_dict['assigned_sessions']:
                self.status_dict['assigned_sessions'].remove(key)
            if key in self.current_sessions:
                del self.current_sessions[key]

    def update_sessions_status(self):
        keys = list(self.sessions_dict.keys())
        self.print("Update {} sessions status".format(len(keys)))
        for k in keys:
            s = self.sessions_dict[k]
            # check process status
            if k not in self.status_dict['current_sessions']:
                self.status_dict['current_sessions'][k] = {
                        'pid': s.pid, 'cpu': 0.0, 'mem': 0.0, 'zombie': 0}
            else:
                if not s.process.is_alive():
                    if s.process.exitcode != 0:
                        time_str = datetime.now().isoformat()
                        self.status_dict['msg'].append(
                            '{} {} is exited with exitcode {}\n'
                            .format(time_str, k, s.process.exitcode))
                    if k in self.status_dict['current_sessions']:
                        del self.status_dict['current_sessions'][k]
                    self.sessions_dict[k].clean_workspace('exiting with error')
                    del self.sessions_dict[k]

            if s.has_finished():
                s.output = s.read_output()
            if s.output != -1:  # remove session
                key = self.get_finished_session_key(k)
                if key:
                    self.status_dict['finished_sessions'][key] = s.output
                if k in self.current_sessions:
                    del self.current_sessions[k]
                if k in self.status_dict['assigned_sessions']:
                    self.status_dict['assigned_sessions'].remove(k)
                if k in self.status_dict['current_sessions']:
                    del self.status_dict['current_sessions'][k]
                if k in self.sessions_dict:
                    # delete session because of finished
                    self.sessions_dict[k].clean_workspace('finished')
                    self.sessions_dict[k].post_process()
                    del self.sessions_dict[k]

    def remove_finished_task(self):
        task_list = self.get_task_list(folder=self.finished_task_folder)
        task_list = list(filter(self.validedTask, task_list))
        task_list += self.get_task_list(
            folder=self.finished_task_folder, ending=".done")
        for task in task_list:
            self.clean_task_trace(task)

    def manual_remove_task(self):
        task_list = self.get_task_list(
            folder=self.finished_task_folder, ending=".delete")
        for task in task_list:
            time_str = self.get_time_str(task)
            task_folder = p_join(self.factory_folder, 'Output', time_str)
            self.clean_task_trace(task)
            self.clean_folder(task_folder, 'manual remove_task', delete=True)

    @staticmethod
    def get_time_str(task):
        return extract_between(task, 'TaskList_', '.json')[0]

    def update_folder_paths(self, task):
        time_str = self.get_time_str(task)
        self.delivery_folder_path = p_join(
            self.delivery_folder, 'Output', time_str)
        self.mat_folder_path = p_join(self.factory_folder, 'Output', time_str)
        self.task_time_str = time_str
#        make_dirs(self.delivery_folder_path)
        make_dirs(self.mat_folder_path)

    def delivery_task(self, target_folder, time_str):
        target_folder = os.path.normpath(target_folder)
        delivery_folder_path = p_join(self.delivery_folder, 'Output', time_str)
        basename = os.path.basename(target_folder)
        target_folder += os.path.sep
        item_list = glob(p_join(target_folder, '**'), recursive=True)
        last_parts = [item.replace(target_folder, '') for item in item_list]
        for i in range(len(item_list)):
            item = item_list[i]
            path_new = p_join(delivery_folder_path, basename, last_parts[i])
            if isdir(item):
                make_dirs(path_new)
            if isfile(item):
                if get_file_suffix(item).lower() != '.mat':
                    shutil.copyfile(item, path_new)

    def clean_task_trace(self, task):
        df = self.get_task_table(task, self.finished_task_folder)
        time_str = self.get_time_str(task)
        unfinished_sessions, output_folder = self.get_unfinished_sessions()
        for session in unfinished_sessions:
            if session in df.index:
                if session in self.sessions_dict:  # kill running session
                    s = self.sessions_dict[session]
                    s.clean_workspace("cleanTaskTrace")
                    self.sessions_dict.pop(session, None)
                else:
                    s = Session(self, session, None, time_str)
                s.post_process()

        for session in df.index:
            if session in self.current_sessions:
                del self.current_sessions[session]
            if session in self.status_dict['assigned_sessions']:
                self.status_dict['assigned_sessions'].remove(session)
            if session in self.status_dict['current_sessions']:
                del self.status_dict['current_sessions'][session]

        keys = list(self.status_dict['finished_sessions'].keys())
        for key in keys:
            if time_str in key:
                del self.status_dict['finished_sessions'][key]

    def clean_unfinished_tasks(self):
        unfinished_sessions, output_folder = self.get_unfinished_sessions()
        for session in unfinished_sessions:
            folder_path = p_join(output_folder, session)
            self.clean_folder(folder_path, 'cleanUnfinishedTasks', delete=True)
        self.current_sessions.clear()
        self.status_dict['current_sessions'].clear()

    def clean_folder(self, folder_name, caller='', delete=False):
        if isdir(folder_name):
            # remove all the content recursively
            self.print("Cleaning folder {} by {}".format(folder_name, caller))
            for filename in os.listdir(folder_name):
                file_path = p_join(folder_name, filename)
                if isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif isdir(file_path):
                    shutil.rmtree(file_path)
            if delete:  # delete this folder
                shutil.rmtree(folder_name)

    def purge_factory(self):
        self.prepare_factory(purge=True)

    def prepare_factory(self, purge=False):
        # purge=True will delete Output folder
        dirsync.sync(self.main_folder, self.factory_folder, 'sync',
                     create=True, exclude=self.excluded_folder, purge=purge)
        make_dirs(p_join(self.factory_folder, 'Output'))

    def cleanSyncConflictState(self):
        states = [f for f in os.listdir(self.server_folder) if f.endswith(
            ".json") if f.startswith(self.host_name)]
        for s in states:
            if 'sync-conflict' in s:
                s_path = p_join(self.server_folder, s)
                if isfile(s_path):
                    os.unlink(s_path)

    def get_task_list(self, folder=None, ending=".json"):
        if folder is None:
            folder = self.new_task_folder
        task_list = [f for f in os.listdir(folder) if f.endswith(ending)]
        return task_list

    def get_task_table(self, task, folder='', unfinished=False):
        if len(folder) == 0:
            folder = self.new_task_folder
        input_path = p_join(folder, task)
        if isfile(input_path):
            df = read_json_to_df(input_path)
            df = df.sort_values('Num')
            df['UUID'] = df['UUID'].apply(str)
            temp = list(
                zip(['Task'] * len(df), df['Num'].apply(str), df['UUID']))
            index = ['-'.join(t) for t in temp]
            df.index = index
            df = df.fillna('')
            if unfinished:
                df = df.drop(df[df['Finished'] == 1].index)
            df2 = df[df['HostName'] == self.host_name]
            df3 = df[df['HostName'] != self.host_name]
            self.update_assigned_task(df2)
            self.remove_redistributed_task(df3)
            return df2
        else:
            return None

    def update_assigned_task(self, df):
        for k in df.index:
            if k not in self.status_dict['assigned_sessions']:
                self.status_dict['assigned_sessions'].append(k)

    def remove_redistributed_task(self, df):
        for k in df.index:
            if k in self.status_dict['assigned_sessions']:
                self.status_dict['assigned_sessions'].remove(k)
                key = self.get_finished_session_key(k)
                if key in self.status_dict['finished_sessions']:
                    del self.status_dict['finished_sessions'][key]
                if k in self.current_sessions:
                    del self.current_sessions[k]
                if k in self.status_dict['current_sessions']:
                    del self.status_dict['current_sessions'][k]
                if k in self.sessions_dict:
                    self.sessions_dict[k].clean_workspace(
                            'remove_redistributed_task')
                    self.sessions_dict[k].delete_relevent_files()
                    del self.sessions_dict[k]
                else:
                    # create a session to delete_relevent_files
                    s = Session(self, k, None, self.task_time_str)
                    s.delete_relevent_files()


if __name__ == '__main__':
    from PyTaskDistributor.util.config import get_host_name, read_config

    config_path = os.path.join(os.getcwd(), 'config.txt')
    config = read_config(config_path)
    hostname = get_host_name()
    setup = config[hostname]
    obj = Server(setup, debug=True)
    obj.main()
    pass
