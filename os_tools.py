""" os_tools.py:
 Description: variuos os commands

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import signal
import sys
import os
import subprocess

def check_running(program,process_name):
    """
        Checks if the process_name is running under the program given
        returns the list of pids found
    """
    pids=[pid for pid in os.listdir('/proc') if pid.isdigit()]
    pids_found=[]
    for pid in pids:
        if int(pid) == int(os.getpid()):
            continue
        try:
            cmd=open(os.path.join('/proc',pid,'cmdline')).read()
            if process_name in cmd and program in cmd:
                pids_found.append(int(pid))
        except IOError:
            continue
    return pids_found

def kill_process(program,process_name):
    """
        Kills the process_name running under the program given
    """
    pids=check_running(program,process_name)
    if not pids:
        return -1
    for pid in pids:
        os.kill(int(pid), signal.SIGKILL)
    return 0

def run_sqlplus(sqlplus_script):
    """
    Run a sql command or group of commands against
    a database using sqlplus.
    """
    p = subprocess.Popen(['sqlplus','-S','/nolog'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    (stdout,stderr) = p.communicate(sqlplus_script.encode('utf-8'))
    # print(stdout.split("\n"))
    # stdout_lines = stdout.decode('utf-8').split("\n")
    stdout_lines = stdout.split("\n")
    return stdout_lines
