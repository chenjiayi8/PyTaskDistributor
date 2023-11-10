""" State module. """
import math


def all_assigned(df):
    """Check if all sessions are assigned."""
    if len(df[df["HostName"] == ""]) == 0:
        return True, "All sessions are assigned\n"
    return False, ""


def is_overwhelmed(skip_flag, cause, server):
    """Check if the server is overwhelmed."""
    if skip_flag:
        return skip_flag, cause
    if (
        server["CPU_total"] > server["CPU_max"]
        or server["MEM_total"] > server["MEM_max"]
    ):
        return True, "Server is overwhelmed\n"
    return False, ""


def not_running(skip_flag, msg_cause, server):
    """Check if the assigned sessions are not running."""
    if skip_flag:
        return skip_flag, msg_cause
    if len(server["current_sessions"]) > server["num_running"]:
        return True, "Assigned sessions are not running\n"
    return False, ""


def not_received(skip_flag, msg_cause, server, df):
    """Check if the assigned sessions are not received."""
    if skip_flag:
        return skip_flag, msg_cause, []
    missed_sessions = []
    df_assigned = df[(df["HostName"] == server["name"]) & (df["Finished"] != 1)]
    for idx in df_assigned.index:
        if idx not in server["current_sessions"]:
            finished_sessions = server["finished_sessions"].keys()
            idx_in_finished_sessions = [idx in s for s in finished_sessions]
            if not any(idx_in_finished_sessions):
                if not skip_flag:
                    msg_cause += "\n"
                skip_flag = True
                msg_cause += f"Assigned session {idx} is not received\n"
                missed_sessions += [idx]

    return skip_flag, msg_cause, missed_sessions


def has_resource(server):
    """Check if the server has enough resource."""
    skip_flag = False
    msg_cause = ""
    num_default = 2
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
        except ValueError:
            num_target = num_default

        num_target = min([num_target, num_default])
        num_target = max([0, num_target])

    # CPU/MEM limit
    if num_target == 0:
        skip_flag = True
        msg_cause += "reaching CPU/MEM limit"

    return skip_flag, msg_cause, num_target
