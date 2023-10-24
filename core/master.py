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
from os.path import isfile, join as p_join
from pathlib import Path

from dateutil import parser
import numpy as np
import pandas as pd

from PyTaskDistributor.util.extract import extract_between
from PyTaskDistributor.util.json import (
    read_json_to_df2 as read_json_to_df,
    read_json_to_dict,
    write_json_from_df2 as write_json_from_df,
)
from PyTaskDistributor.util.monitor import Monitor
from PyTaskDistributor.util.others import (
    sleep_mins,
    update_xlsx_file,
    get_uuid,
    delete_file,
    send_email,
    make_dirs,
)


class Master:
    def __init__(self, setup, fast_mode=False):
        self.setup = setup
        self.default_folder = os.getcwd()
        self.fast_mode = fast_mode
        self.master_folder = p_join(self.default_folder, "Masters")
        self.server_folder = p_join(self.default_folder, "Servers")
        self.log_file = p_join(self.master_folder, f"{setup['hostname']}.txt")
        self.main_folder = self.setup["order"]
        self.new_task_folder = p_join(self.main_folder, "NewTasks")
        self.finished_task_folder = p_join(self.main_folder, "FinishedTasks")
        self.backup_task_folder = p_join(self.main_folder, "FinishedTasks", "Backup")
        self.task_file_path = p_join(self.main_folder, "TaskList.xlsx")
        self.last_modified_time = ""
        self.monitor = Monitor(self)
        self.server_list = []
        self.server_list_offline = {}
        self.server_list_overflow = {}
        self.msgs = []
        self.initialise()

    def initialise(self):
        make_dirs(self.master_folder)
        make_dirs(self.server_folder)
        print("Master {} started".format(self.setup["hostname"]))

    def main(self):
        num_min = random.randint(3, 5)
        try:
            if self.last_modified_time != self.get_file_last_modified_time():
                self.generate_tasks()
            self.update_server_list()
            self.generate_manual_tasks()
            self.generate_clean_tasks()
            self.generate_purge_tasks()
            self.remove_finished_task()
            self.remove_aborted_task()
            self.workload_balance()
            self.update_task_status()
            self.print_progress(num_min)
            self.print_msgs()
            sleep_mins(num_min)
            need_assistance = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            print("Need assistance for unexpected error:\n {}".format(sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            need_assistance = True
        return need_assistance

    def print_progress(self, num_min=5):
        msg = self.monitor.print_progress(num_min)
        self.msgs.insert(0, msg)

    def print_msgs(self):
        with open(self.log_file, "w") as f:
            for msg in self.msgs:
                f.write(msg + "\n")
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

        def validedTask(task):  # skip these task for another server
            task_lower = task.lower()
            if task_lower.endswith("clean.json"):
                return False
            if task_lower.endswith("purge.json"):
                return False
            if "sync-conflict" in task_lower:
                delete_file(p_join(folder, task))
                return False
            return True

        task_list = list(filter(validedTask, task_list))
        return task_list

    def update_server_list(self, timeout_mins=30):
        server_list = [f for f in os.listdir(self.server_folder) if f.endswith(".json")]
        self.server_list.clear()
        for s in server_list:
            try:
                s_path = p_join(self.server_folder, s)
                if "sync-conflict" in s:
                    if isfile(s_path):
                        os.unlink(s_path)
                else:
                    s_json = read_json_to_dict(s_path)
                    s_time = parser.parse(s_json["updated_time"])
                    diff_min = (datetime.now() - s_time).seconds / 60
                    # only use if updated within N mins
                    if diff_min < timeout_mins:
                        self.server_list.append(s_json)
            except Exception:
                self.msgs.append("Failed to read status of {}".format(s))
                pass

    def reset_offline_task(self, df):
        # workbalance due to offline servers
        df_assigned_total = df.loc[(df["HostName"] != "") & (df["Finished"] != 1)]
        server_with_tasks = list(set(df_assigned_total["HostName"].tolist()))
        server_online = [s["name"] for s in self.server_list]
        server_offline = [s for s in server_with_tasks if s not in server_online]

        # remove server from offline list if it is online again
        for s in server_online:
            if s in self.server_list_offline:
                del self.server_list_offline[s]

        # count the time a server is offline
        for s in server_offline:
            if s in self.server_list_offline:
                self.server_list_offline[s] += 1
            else:
                self.server_list_offline[s] = 1

        # remove the tasks from a server for too long time (about 40 mins)
        del_list = []
        for s, v in self.server_list_offline.items():
            # remove offline server without tasks
            if s not in server_online and s not in server_offline:
                del_list.append(s)
            if v > 10:
                df.loc[(df["HostName"] == s) & (df["Finished"] != 1), "HostName"] = ""
                del_list.append(s)
                msg = "Reset tasks due to offline of {}".format(s)
                self.msgs.append(msg)

        for s in del_list:
            del self.server_list_offline[s]

        return df

    def server_overflow(self, server):
        redistributed = False
        if server["name"] not in self.server_list_overflow:
            self.server_list_overflow[server["name"]] = 1
        else:
            self.server_list_overflow[server["name"]] += 1

        if self.server_list_overflow[server["name"]] > 10:
            self.server_list_overflow[server["name"]] = 0
            redistributed = True

        return redistributed

    def workload_balance(self):
        num_default = 2
        task_list = self.get_task_list()
        for task in task_list:
            if "sync-conflict" in task:  # conflict file from Syncthing
                task_path = p_join(self.new_task_folder, task)
                os.unlink(task_path)
        if len(task_list) == 0:
            return
        task = random.choice(task_list)  # Only balance one task per time
        task_path = p_join(self.new_task_folder, task)
        df = read_json_to_df(task_path)
        df = df.sort_values("Num")
        df["UUID"] = df["UUID"].apply(str)
        temp = list(zip(["Task"] * len(df), df["Num"].apply(str), df["UUID"]))
        index = ["-".join(t) for t in temp]
        df.index = index
        df = df.fillna("")
        df = self.reset_offline_task(df)
        fasted_flag = self.fast_mode
        # override fastmode to True for small task
        if len(df) <= len(self.server_list) * num_default:
            fasted_flag = True
        num_server = len(self.server_list)
        # assign tasks among online servers
        for i in range(num_server):
            server = self.server_list[i]
            num_target = 0
            msg_cause = ""

            # check if server is overwhelmed
            if (
                server["CPU_total"] > server["CPU_max"]
                or server["MEM_total"] > server["MEM_max"]
            ):
                skip_flag = True
                msg_cause += "Server is overwhelmed\n"
            else:
                skip_flag = False

            # No more sessions
            if not skip_flag:
                df_temp = df[df["HostName"] == ""]
                if len(df_temp) == 0:
                    msg_cause += "All sessions are assigned\n"
                    skip_flag = True

            # assigned sessions but not running
            if not skip_flag:
                if len(server["current_sessions"]) > server["num_running"]:
                    skip_flag = True
                    msg_cause += "Assigned sessions are not running\n"

            # assigned sessions but not received
            df_assigned = df[(df["HostName"] == server["name"]) & (df["Finished"] != 1)]
            for idx in df_assigned.index:
                if idx not in server["current_sessions"]:
                    finished_sessions = server["finished_sessions"].keys()
                    idx_in_finished_sessions = [idx in s for s in finished_sessions]
                    if not any(idx_in_finished_sessions):
                        if not skip_flag:
                            msg_cause += "\n"
                        skip_flag = True
                        msg_cause += (
                            "Assigned session {} of {} is not " "received\n"
                        ).format(idx, task)
                        if self.server_overflow(server):
                            msg_cause += (
                                "Assigned session {} of {} is " "redistributed\n"
                            ).format(idx, task)
                            df.loc[idx, "HostName"] = ""

            if not skip_flag:
                if int(server["num_running"]) == 0:
                    num_target = num_default
                else:
                    try:
                        cpu_available = server["CPU_max"] - server["CPU_total"]
                        cpu_per_task = server["CPU_matlab"] / server["num_running"]
                        num_cpu = math.floor(cpu_available / cpu_per_task)
                        mem_available = server["MEM_max"] - server["MEM_total"]
                        mem_per_task = server["MEM_matlab"] / server["num_running"]
                        num_mem = math.floor(mem_available / mem_per_task)
                        num_target = min([num_cpu, num_mem])
                    except Exception:
                        num_target = num_default
                    # Max add num_default per cycle
                    if num_target > num_default:
                        num_target = num_default
                    if num_target < 0:
                        num_target = 0
                # CPU/MEM limit
                if num_target == 0:
                    skip_flag = True
                    msg_cause += "reaching CPU/MEM limit"

            # Do not assign new session
            if skip_flag:
                msg = "Assign 0 new sessions of {} for Server {} because: {}".format(
                    task, server["name"], msg_cause
                )
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
                    df.loc[index[ii], "HostName"] = server["name"]
                msg = "Assign {} new sessions of {} for Server {}".format(
                    num_target, task, server["name"]
                )
            # record msg
            self.msgs.append(msg)
        write_json_from_df(task_path, df)

    def get_finished_sessions(self):
        finished_sessions = {}
        for server in self.server_list:
            temp_dict = server["finished_sessions"]
            for _, v in temp_dict.items():
                v["HostName"] = server["name"]
            finished_sessions.update(temp_dict)
        return finished_sessions

    @staticmethod
    def get_time_str(task):
        return extract_between(task, "TaskList_", ".json")[0]

    @staticmethod
    def read_task(path):
        df = read_json_to_df(path)
        df = df.sort_values("Num")
        df["UUID"] = df["UUID"].apply(str)
        temp = list(zip(["Task"] * len(df), df["Num"].apply(str), df["UUID"]))
        index = ["-".join(t) for t in temp]
        df.index = index
        df = df.fillna("")
        return df

    @staticmethod
    def mark_task_df(task, df, finished_sessions):
        modified_flag = False
        for key in finished_sessions.keys():
            underline_positions = [i for i, ltr in enumerate(key) if ltr == "_"]
            task_time_str = key[: underline_positions[1]]
            index = key[underline_positions[1] + 1 :]
            if task_time_str in task and index in df.index:
                # only modify when necessary
                if df.loc[index, "Finished"] != 1:
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
            path_xlsx = p_join(self.main_folder, "Output", task[:-5] + ".xlsx")
            df = self.read_task(path)
            modified_flag = self.mark_task_df(task, df, finished_sessions)
            if modified_flag:
                write_json_from_df(path, df)
                update_xlsx_file(path_xlsx, df)
            if all(df["Finished"] == 1):  # rename to json.done
                path_done = p_join(self.finished_task_folder, task + ".done")
                shutil.move(path, path_done)
                main_folder = Path(self.main_folder).parts[-1]
                send_email("Finished task", "{}: {} is done".format(main_folder, task))

    def remove_aborted_task(self):
        task_list = self.get_task_list(folder=self.finished_task_folder, ending=".done")
        task_list += self.get_task_list(
            folder=self.finished_task_folder, ending=".delete"
        )
        task_list = self.get_task_list(
            folder=self.finished_task_folder, ending=".done.json"
        )
        task_list += self.get_task_list(
            folder=self.finished_task_folder, ending=".delete.json"
        )
        for task in task_list:
            task_time_str = extract_between(task, "TaskList_", ".")[0]
            json_name = "TaskList_" + task_time_str + ".json"
            path = p_join(self.new_task_folder, json_name)
            if isfile(path):
                delete_file(path)
                self.msgs.append(
                    "Delete {} because it is aborted/finished.".format(json_name)
                )

    def update_task_status(self):
        task_list = self.get_task_list()
        finished_sessions = self.get_finished_sessions()
        for task in task_list:
            path = p_join(self.new_task_folder, task)
            path_xlsx = p_join(self.main_folder, "Output", task[:-5] + ".xlsx")
            df = self.read_task(path)
            modified_flag = self.mark_task_df(task, df, finished_sessions)
            if modified_flag:
                write_json_from_df(path, df)
                update_xlsx_file(path_xlsx, df)
            if all(df["Finished"] == 1):  # move to finish folder
                path_done = p_join(self.finished_task_folder, task)
                shutil.move(path, path_done)

    def get_existed_task(self):
        path_list = []
        folder_list = [
            self.finished_task_folder,
            self.new_task_folder,
            self.backup_task_folder,
        ]
        endings = ["json", "done", "delete"]
        for folder in folder_list:
            for ending in endings:
                file_list = self.get_task_list(folder=folder, ending=ending)
                path_list += [p_join(folder, file) for file in file_list]
        return path_list

    def exist_task(self, task_name):
        path_list = self.get_existed_task()
        file_list = [Path(p).parts[-1] for p in path_list]
        return any([task_name in file for file in file_list])

    def get_exist_task_path(self, json_name):
        path_list = self.get_existed_task()
        path_list = [p for p in path_list if json_name in p]
        return path_list

    def generate_tasks(self):
        if not isfile(self.task_file_path):
            return
        self.last_modified_time = self.get_file_last_modified_time()
        last_modified_time_str = datetime.strftime(
            self.last_modified_time, "%Y%m%d_%H%M%S"
        )
        task_name = "TaskList_" + last_modified_time_str
        json_name = task_name + ".json"
        if self.exist_task(task_name):  # already created
            return
        new_json_name = p_join(self.new_task_folder, json_name)
        modified_time = os.path.getmtime(self.task_file_path)
        parameter_table = pd.read_excel(
            self.task_file_path, sheet_name="ParameterRange", engine="openpyxl"
        )
        parameter_names = list(parameter_table.columns)
        num_total_task = 1
        for name in parameter_names:
            vector = parameter_table.loc[:, name]
            num_non_nan = list(np.isnan(vector)).count(False)
            num_total_task *= num_non_nan

        columns = ["Num"] + parameter_names
        vector_list = []
        for name in parameter_names:
            vector = parameter_table.loc[:, name]
            non_nan_idx = np.isnan(vector)
            non_nan_idx = [not i for i in non_nan_idx]
            vector = [float(vector[i]) for i in range(len(vector)) if non_nan_idx[i]]
            vector_list.append(vector)

        combinations = list(prod(*vector_list))
        combinations = [
            [i + 1] + list(combinations[i]) for i in range(len(combinations))
        ]
        task_table = pd.DataFrame(data=combinations, columns=columns)
        task_table_old = pd.read_excel(
            self.task_file_path, sheet_name="Sheet1", engine="openpyxl"
        )
        columns_old = list(task_table_old.columns)
        for c in columns_old:
            if c not in task_table.columns:
                task_table[c] = ""
        task_table.loc[:, "UUID"] = task_table.loc[:, "UUID"].apply(get_uuid)
        update_xlsx_file(self.task_file_path, task_table)
        task_table = pd.read_excel(
            self.task_file_path, sheet_name="Sheet1", engine="openpyxl"
        )
        new_xlsx_name = p_join(
            self.main_folder, "Output", "TaskList_" + last_modified_time_str + ".xlsx"
        )
        output_folder = p_join(self.main_folder, "Output", last_modified_time_str)
        make_dirs(output_folder)
        write_json_from_df(new_json_name, task_table)
        shutil.copyfile(self.task_file_path, new_xlsx_name)
        os.utime(self.task_file_path, (modified_time, modified_time))
        os.utime(new_json_name, (modified_time, modified_time))
        os.utime(new_xlsx_name, (modified_time, modified_time))

    def removeResidualTasks(self, cleanOrPurgeTask):
        remove_time = self.get_file_last_modified_time(cleanOrPurgeTask)
        resident_tasks = self.get_task_list(self.new_task_folder)
        for task in resident_tasks:
            task_time_str = extract_between(task, "TaskList_", ".")[0]
            task_time = datetime.strptime(task_time_str, "%Y%m%d_%H%M%S")
            # move old tasks to finished task folder
            if remove_time >= task_time:
                self.msgs.append("Deleting old task {}".format(task))
                old_path = p_join(self.new_task_folder, task)
                new_path = p_join(self.finished_task_folder, task + ".done")
                shutil.move(old_path, new_path)

    def generate_manual_tasks(self):
        manual_folder = p_join(self.main_folder, "ManualTasks")
        manual_xlsx = p_join(manual_folder, "TaskList_manual.xlsx")
        if not isfile(manual_xlsx):
            return
        last_modified_time = self.get_file_last_modified_time(manual_xlsx)
        last_modified_time_str = datetime.strftime(last_modified_time, "%Y%m%d_%H%M%S")
        task_name = "TaskList_" + last_modified_time_str
        json_name = task_name + ".json"
        if not self.exist_task(task_name):  # already created
            print("Generating manual tasks")
            new_json_name = p_join(self.new_task_folder, json_name)
            df = pd.read_excel(manual_xlsx)
            df = df.fillna("")
            df.loc[:, "UUID"] = df.loc[:, "UUID"].apply(get_uuid)
            write_json_from_df(new_json_name, df)

        manual_jsons = self.get_task_list(manual_folder)
        for task in manual_jsons:
            exist_paths = self.get_exist_task_path(task)
            for path in exist_paths:
                delete_file(path)  # delete exist task
            task_path = p_join(manual_folder, task)
            df = read_json_to_df(task_path)
            df["Finished"] = 0  # reset task status
            task_path_new = p_join(self.new_task_folder, task)
            write_json_from_df(task_path_new, df)  # create new task
            path_xlsx = p_join(self.main_folder, "Output", task[:-5] + ".xlsx")
            update_xlsx_file(path_xlsx, df)
            delete_file(p_join(manual_folder, task))  # delete manual task

    def generate_clean_tasks(self):
        clean_task = p_join(self.new_task_folder, "clean.json")
        if isfile(clean_task):
            self.msgs.append("Generating clean tasks")
            self.removeResidualTasks(clean_task)
            delete_file(clean_task)
            for server in self.server_list:
                clean_server_path = p_join(
                    self.new_task_folder, server["name"] + "_clean.json"
                )
                Path(clean_server_path).touch()

    def generate_purge_tasks(self):
        purge_task = p_join(self.new_task_folder, "purge.json")
        if isfile(purge_task):
            self.msgs.append("Generating purge tasks")
            self.removeResidualTasks(purge_task)
            delete_file(purge_task)
            for server in self.server_list:
                purge_server_path = p_join(
                    self.new_task_folder, server["name"] + "_purge.json"
                )
                Path(purge_server_path).touch()


if __name__ == "__main__":
    pass
