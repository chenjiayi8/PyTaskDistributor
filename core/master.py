#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 10:05:04 2020

@author: frank
"""

import os
import random
import traceback
from datetime import datetime
from os.path import isfile
from os.path import join as p_join

from dateutil import parser

from PyTaskDistributor.core.simulator import Simulator
from PyTaskDistributor.util.json import read_json_to_dict
from PyTaskDistributor.util.monitor import Monitor
from PyTaskDistributor.util.others import make_dirs, sleep_mins


class Master:
    def __init__(self, simulator: Simulator):
        self.default_folder = os.getcwd()
        self.simulator = simulator
        self.hostname = self.simulator.hostname
        self.monitor = Monitor(self)
        self.master_folder = p_join(self.default_folder, "Masters")
        self.server_folder = p_join(self.default_folder, "Servers")
        self.log_file = p_join(self.master_folder, f"{self.hostname}.txt")
        self.servers = []
        self.msgs = []
        self.initialise()

    def initialise(self):
        """Initialise the master."""
        make_dirs(self.master_folder)
        make_dirs(self.server_folder)
        print(f"Master {self.hostname} started")
        self.update_servers(10000)

    def main(self):
        """Main method of the master."""
        try:
            num_min = random.randint(3, 5)
            self.msgs += self.simulator.generate()
            self.msgs += self.simulator.dispatch(self.servers)
            tasks = self.simulator.summarise()
            self.check_progress(tasks, num_min)
            self.print_progress()
            self.update_servers(10000)
            sleep_mins(num_min)
            need_assistance = False
        except (KeyboardInterrupt, SystemExit):
            print("KeyboardInterrupt or SystemExit")
            raise
        except (ValueError, TypeError, ZeroDivisionError) as e:
            print(f"Need assistance for unexpected error:\n {e}")
            traceback.print_exc()
            need_assistance = True
        return need_assistance

    def check_progress(self, tasks, num_min=5):
        """Check the progress of the master."""
        msg = self.monitor.check_progress(tasks, self.servers, num_min)
        self.msgs.insert(0, msg)

    def print_progress(self):
        """Print the messages of the master."""
        with open(self.log_file, "w", encoding="utf-8") as f:
            for msg in self.msgs:
                f.write(msg + "\n")
                print(msg)
        self.msgs.clear()

    def update_servers(self, timeout_mins=30):
        """Update the server list."""
        files = [f for f in os.listdir(self.server_folder) if f.endswith(".json")]
        self.servers.clear()
        for file in files:
            path = p_join(self.server_folder, file)
            if "sync-conflict" in file:
                if isfile(path):
                    os.unlink(path)
            else:
                server = read_json_to_dict(path)
                if server is None:
                    continue
                s_time = parser.parse(server["updated_time"])
                diff_min = (datetime.now() - s_time).seconds / 60
                # only use if updated within N mins
                if diff_min < timeout_mins:
                    self.servers.append(server)

    def distribute(self, tasks):
        pass


if __name__ == "__main__":
    pass
