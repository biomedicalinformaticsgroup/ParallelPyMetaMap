import time
from datetime import datetime
import os
import signal

from ParallelPyMetaMap.timeout.job_summary import job_summary

def timeout_metamap_process(timeout = 10800, username = None):
    
    if timeout < 300:
        print(f'\ntimeout too low, raising timeout to 5 minutes\n')
        timeout = 300
        
    regular_check = 600
    if timeout <= regular_check:
        regular_check = timeout * 0.9

    time.sleep(regular_check)

    now = datetime.now()
    print(f'\n\n----------------------------------\n\nThe Time of this check is {now}')

    result, running = job_summary(username)

    while running == True:

        job_kill = []
        time_kill = []
        for i in range(len(result)):
            if int(result.iloc[i].time.split(':')[0])*60 + int(result.iloc[i].time.split(':')[1]) >= timeout:
                job_kill.append(result.iloc[i].pid)
                time_kill.append(result.iloc[i].time)
        
        print(f'{len(result)} processe(s) is/are currently working')
        print(f'{len(job_kill)} processe(s) exceeded timeout.')

        if len(job_kill) > 0:
            print('Now we will abort this/these processe(s)')
            for i in range(len(job_kill)):
                try:
                    os.kill(int(job_kill[i]), signal.SIGTERM)
                    print(f'Killing job id {job_kill[i]} after {time_kill[i]} minutes\n')
                    time.sleep(5)
                except:
                    pass
        else:
            print(f'All jobs are working correctly\n')
            
        next_check = []    
        for i in range(len(result)):
            next_check.append(int(result.iloc[i].time.split(':')[0])*60 + int(result.iloc[i].time.split(':')[1]))
        
        regular_check = timeout - max(next_check)
        regular_check = regular_check + 1
        
        if regular_check < 0:
            regular_check = 0

        time.sleep(regular_check)
        now = datetime.now()
        print(f'\n\n----------------------------------\n\nThe Time of this check is {now}')
        result, running = job_summary(username)

    else:

        now = datetime.now()
        print(f'\n\n----------------------------------\n\nThe Time of this check is {now}')
        print(f'All jobs finished\n')