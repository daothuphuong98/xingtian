import subprocess
import signal
import os
import time
import sys
sys.path.append(r"simulator/dpdp_competition")

with open('ortools_opt/main.log', 'w') as f:
    p = subprocess.Popen(["python3", "ortools_opt/main.py"], stdout=f, shell=False)
    time.sleep(100)
    # Get the process id
    pid = p.pid
    os.kill(pid, signal.SIGINT)

if not p.poll():
    time.sleep(2)
    print("SUCCESS")

# print(os.stat('/workspaces/xingtian/simulator/dpdp_competition/algorithm/data_interaction/output_destination.json').st_mtime)