import time
from datetime import datetime
import pandas as pd
import subprocess
import os
import signal

def job_summary(username = None):
    print('Checking for jobs')
    result = None
    command = subprocess.getstatusoutput("ps -aux | grep 'metamap..\.BINARY'")
    command = list(command)
    if command[1] == '':
        command = []
    else:
        command = command[1]
        my_list = []
        for i in range(len(str(command).split('\n'))):
            my_list.append(str(command).split('\n')[i])
        command = my_list
        jobs = command
    if len(jobs) > 0:
        
        print(f'looks like the job ParallelPyMetaMap is still running')
        running = True
        user = []
        pid = []
        time = []
        for i in range(len(jobs)):
            user.append(jobs[i].split()[0])
            pid.append(jobs[i].split()[1])
            time.append(jobs[i].split()[9])
        
        dict = {'user': user, 'pid': pid, 'time': time} 
        result = pd.DataFrame(dict)
        
        if username != None:
            result = result[result.user == username]
        if len(result) == 0:
            print('looks like the job is no longer running for the specified user')
            running = False
        
    else:
        print('looks like the job is no longer running')
        runnning = False
    return result, running