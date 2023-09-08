#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 11:16:30 2021

@author: frank
"""

import os
import random
import shutil
import sys
import time
import traceback
import dirsync
from datetime import datetime
from glob import glob
from threading import Thread
from os.path import basename, isdir, isfile, join as p_join

import matlab.engine

from PyTaskDistributor.util.json import read_json_to_dict
from PyTaskDistributor.util.others import (
    get_file_suffix, get_latest_file_in_folder, make_dirs, get_process_cpu)


class Session:
    def __init__(self, server, name, _input, timestr=None):
        self.server = server
        self.name = name
        self.input = _input
        self.is_unfinished = type(self.input) is str
        self.default_folder = server.default_folder
        self.main_folder = server.main_folder
        self.factory_folder = server.factory_folder
        self.working_folder = p_join(self.factory_folder, 'Output', name)
        self.delivery_folder_path = server.delivery_folder_path
        if timestr is None:
            self.mat_folder_path = server.mat_folder_path
        else:  # finished session from another task
            self.mat_folder_path = p_join(self.factory_folder,
                                          'Output', timestr)
        self.logFile = p_join(self.factory_folder, 'Output',
                              name, name + '.txt')
        self.process = None
        self.pid = -1
        self.eng = None
        self.output = -1
        self.zombieState = 0
        self.terminated = False

    def create_log_file(self):
        if not isfile(self.logFile):
            basedir = os.path.dirname(self.logFile)
            make_dirs(basedir)
            with open(self.logFile, 'a'):  # touch file
                os.utime(self.logFile, None)

    def initialise(self):
        make_dirs(self.working_folder)
        self.create_log_file()
        self.write_log("Session {} initialised at Server {}".format(self.name,
                       self.server.host_name))

    def write_log(self, msg):
        time_prefix = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')
        time_prefix = time_prefix[:-3] + '$ '
        platform_prefix = 'Python: '
        msg = time_prefix + platform_prefix + msg + '\n'
        print(msg)
        try:
            if isfile(self.logFile):
                with open(self.logFile, 'a+') as f:
                    f.write(msg)
        except Exception:
            pass

    def clean_workspace(self, caller=''):
        if not self.terminated:
            if len(caller) > 0:
                self.write_log('clean_workspace called for {} because {}'
                               .format(self.name, caller))
            else:
                self.write_log('clean_workspace called for {}'
                               .format(self.name))
            self.terminated = True

    def __del__(self):
        # killed or not initialised
        if self.pid == -1:
            return

        # matlab eng api
        try:
            self.eng.quit()
        except Exception:
            self.write_log("Cannot quit matlab_eng for {} ".format(self.name))
            pass

        # garbage collection
        self.pid = -1
        self.eng = None

    def create_matlab_eng(self, option=None):
        os.chdir(self.factory_folder)
        self.initialise()
        exitcode = 0
        if type(self.input) is str:
            caller = 'old task'
        else:
            caller = 'new task'
        self.write_log("Creating matlab engine for {} {}"
                       .format(caller, self.name))
        try:
            if option is None:
                self.eng = matlab.engine.start_matlab()  # option: '-desktop'
            else:
                self.eng = matlab.engine.start_matlab(option)

            self.pid = int(self.eng.eval("feature('getpid')"))
            self.write_log("Creating matlab engine for {} {} done with pid {}"
                           .format(caller, self.name, self.pid))
        except (KeyboardInterrupt, SystemExit):
            exitcode = -1
            raise
        except Exception as e:
            exitcode = -2
            self.write_log("Failed to create matlab engine with message:\n {}"
                           .format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            self.write_log(str(e))
            self.write_log(traceback.format_exc())
            self.server.deal_with_failed_session(self.name)

        return exitcode

    def delivery_task(self, target_folder):
        self.write_log("delivery_task started for {}".format(self.name))
        target_folder = os.path.normpath(target_folder)
        base_name = basename(target_folder)
        target_folder += os.path.sep
        item_list = glob(p_join(target_folder, '**'), recursive=True)
        last_parts = [item.replace(target_folder, '') for item in item_list]
        for i in range(len(item_list)):
            item = item_list[i]
            path_new = p_join(self.delivery_folder_path,
                              base_name, last_parts[i])
            if isdir(item):
                make_dirs(path_new)
            if isfile(item):
                if get_file_suffix(item).lower() != '.mat':
                    shutil.copyfile(item, path_new)
        self.write_log("delivery_task finished for {}".format(self.name))

    def delete_relevant_files(self):
        delivery_folder = p_join(self.mat_folder_path, self.name)
        factory_folder = p_join(self.factory_folder, 'Output', self.name)
        self.server.clean_folder(delivery_folder, 'delete_relevant_files',
                                 delete=True)
        self.server.clean_folder(factory_folder, 'delete_relevant_files',
                                 delete=True)

    def get_output(self, suffix):
        data_folder = p_join(self.working_folder, 'data')
        output_file = get_latest_file_in_folder(data_folder, suffix)
        return output_file

    def get_json_output(self):
        return self.get_output('.json')

    def get_mat_output(self):
        return self.get_output('.mat')

    def read_output(self, json_file=None):
        if json_file is None:
            json_file = self.get_json_output()
        if json_file is not None:
            output = read_json_to_dict(json_file)
            return output
        else:
            return None

    @staticmethod
    def file_not_updating(file, seconds=30):
        m_time = os.path.getmtime(file)
        interval = 5
        while seconds > 0:
            time.sleep(interval)
            seconds -= interval
            if m_time != os.path.getmtime(file):
                return False  # fail fast
        return True

    def has_low_cpu(self):
        if self.pid == -1:
            return True
        if get_process_cpu(self.pid) < 10:
            return True
        else:
            return False

    def has_finished(self):
        if not self.has_low_cpu():
            return False
        json_file = self.get_json_output()
        mat_file = self.get_mat_output()
        if json_file is None:
            return False
        if '-err.json' in json_file:
            return True
        if mat_file is None:
            return False
        if basename(json_file).lower() != 'final.json':
            return False
        result = self.read_output(json_file)
        if 'Finished' not in result:
            return False
        if result['Finished'] == 1.0:
            return True
        return False

    def run_matlab_unfinished_task(self):
        time.sleep(random.randint(1, 30))
        self.write_log("run_matlab_unfinished_task with input {}"
                       .format(self.input))
        self.eng.MatlabToPyRunUnfinishedTasks([self.input, self.logFile],
                                              nargout=0)
        self.write_log("run_matlab_unfinished_task finished")

    def run_matlab_new_task(self):
        self.write_log("run_matlab_new_task with input {}".format(self.input))
        self.eng.MatlabToPyRunNewTasks(self.input, nargout=0)
        self.write_log("run_matlab_new_task finished")

    def run_matlab_task(self):
        os.chdir(self.factory_folder)
        input_type = type(self.input)
        self.write_log("run_matlab_task for {}".format(self.name))
        if input_type is str:
            self.run_matlab_unfinished_task()
        else:
            self.run_matlab_new_task()
        self.write_log("run_matlab_task for {} done".format(self.name))

    def post_process(self):
        self.write_log("post_process started for {}".format(self.name))
        target_folder = p_join(self.mat_folder_path, self.name)
        source_folder = p_join(self.factory_folder, 'Output', self.name)
        # copy simulation results to task result folder
        dirsync.sync(source_folder, target_folder, 'sync', create=True)
        # delivery everything excluding mat file
        self.write_log('Ready to delivery result')
        time.sleep(3)
        self.delivery_task(target_folder)
        self.server.clean_folder(source_folder, 'postProcess in Session',
                                 delete=True)
        os.chdir(self.default_folder)
        # write last log in task result folder
        self.logFile = p_join(self.mat_folder_path,
                              self.name, self.name + '.txt')
        self.write_log(
            "post_process finished for {} in simulation folder".format(self.name))
        # write last log in delivery folder
        self.logFile = p_join(self.delivery_folder_path,
                              self.name, self.name + '.txt')
        self.write_log(
            "post_process finished for {} in delivery folder".format(self.name))

    def main(self, target=''):
        if len(target) == 0:
            target = 'run'
        # call function by string
        self.process = Thread(target=getattr(self, target))
        self.write_log("Process to run Function {} of {} is created"
                       .format(target, self.name))
        self.process.start()
        self.write_log("Function {} of {} is running in background"
                       .format(target, self.name))

    def run(self):
        try:
            self.write_log("Working on {}".format(self.name))
            self.run_matlab_task()
            self.write_log("Finishing {}".format(self.name))
        except (KeyboardInterrupt, SystemExit):
            self.clean_workspace('killed by user or at exit')
            raise
        except Exception as e:
            self.write_log("Need assistance for unexpected error:\n {}"
                           .format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            self.write_log(str(e))
            self.write_log(traceback.format_exc())
            self.server.deal_with_failed_session(self.name)


if __name__ == '__main__':
    pass
