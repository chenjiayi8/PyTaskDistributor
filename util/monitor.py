#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 00:38:06 2021

@author: frank
"""

from datetime import datetime

import pandas as pd
from dateutil import parser

from PyTaskDistributor.util.others import print_table


def parse_time(t):
    """Parse time to string."""
    if isinstance(t, str):
        t = parser.isoparse(t)
    elif isinstance(t, float):
        t = datetime.fromtimestamp(t)
    return datetime.strftime(t, "%d/%m/%Y %H:%M:%S")


def get_time_str(task):
    """Get the time string from task name."""
    mark_location = [i for i, ltr in enumerate(task) if ltr == "_"]
    time_str = task[mark_location[0] + 1 :]
    return time_str


def get_duplicated_items(lst1, lst2):
    """Get duplicated items from two lists."""
    return set(lst1) & set(lst2)


def is_server_state(_input):
    """Check if the input is a server state."""
    return any(x in _input for x in ["CPU", "MEM", "DISK"])


class Monitor:
    """Monitor the progress of the master."""

    def __init__(self, master):
        self.master = master
        self.columns_server = [
            "name",
            "CPU_max",
            "MEM_max",
            "CPU_total",
            "MEM_total",
            "DISK_total",
            "CPU_matlab",
            "MEM_matlab",
            "num_assigned",
            "num_running",
            "num_finished",
            "updated_time",
        ]
        self.columns_task = [
            "name",
            "num_task",
            "num_assigned",
            "num_running",
            "num_finished",
            "updated_time",
        ]

    def check_progress(self, tasks: dict, servers: list, num_mins: int = 5):
        """Check the progress of the master."""
        time_str = parse_time(datetime.now())
        msg = f"Updated on {time_str} and will reload in {num_mins} mins \n"
        msg += self.check_task_progress(tasks, servers)
        msg += self.check_server_progress(servers)
        return msg

    def check_task_progress(self, tasks: dict, servers: list):
        """Check the progress of the task."""
        msg = "Task:\n"
        df_tasks = pd.DataFrame(columns=self.columns_task)
        assigned_sessions = []
        finished_sessions = []
        current_sessions = []
        for server in servers:
            assigned_sessions += server["assigned_sessions"]
            finished_sessions += server["finished_sessions"].keys()
            current_sessions += server["current_sessions"].keys()
        for task_name in tasks.keys():
            df_task, updated_time = tasks[task_name]
            time_str = get_time_str(task_name)
            df_task = df_task.sort_values("Num")
            df_task["UUID"] = df_task["UUID"].apply(str)
            temp = list(
                zip(["Task"] * len(df_task), df_task["Num"].apply(str), df_task["UUID"])
            )
            index1 = ["-".join(t) for t in temp]
            index2 = [time_str + "_" + "-".join(t) for t in temp]
            num_task = len(df_task)
            num_assigned = len(get_duplicated_items(assigned_sessions, index1))
            num_finished = len(get_duplicated_items(finished_sessions, index2))
            num_running = len(get_duplicated_items(current_sessions, index1))
            updated_time_str = parse_time(updated_time)
            data = [
                task_name,
                num_task,
                num_assigned,
                num_running,
                num_finished,
                updated_time_str,
            ]
            df_tasks = pd.concat(
                [df_tasks, pd.DataFrame(data=[data], columns=self.columns_task)]
            )
        msg += print_table(df_tasks)
        return msg

    def check_server_progress(self, servers: list):
        """Check the progress of the server."""
        msg = "Server:\n "
        df = pd.DataFrame(columns=self.columns_server)
        for server in servers:
            data = [server[k] for k in self.columns_server]
            df = pd.concat([df, pd.DataFrame(data=[data], columns=self.columns_server)])
        df["updated_time"] = df["updated_time"].apply(parse_time)
        df = df.fillna(0)
        columns_new = [a + "(%)" if is_server_state(a) else a for a in df.columns]
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
                    p.insert(0, "")
            for j, p in enumerate(part):
                part[j] = [col_head[j]] + p
            parts += part
        msg += print_table(parts)
        return msg


if __name__ == "__main__":
    pass
