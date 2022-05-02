
import psutil
import re
from subprocess import Popen, PIPE
import socket
import pandas as pd
import multiprocessing
import threading
import os

csv = 'disk_usage.csv'
user = 'kuntai'
lock = multiprocessing.Lock()
hostname = socket.gethostname()
print('Hostname is ' + hostname)

def calc_usage(disk):
        
    data = []
    
    process = Popen([
        'du', '-d', '5', '-h', disk + '/' + user
    ], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    output = stdout.decode()
    
    print('du finishes on ' + disk)
    
    for line in output.split('\n'):
        if line == "":
            continue
        line = line.split('\t')
        size, path = line[0], line[1]
        depth = path.count('/')
        data.append({
            'size': size,
            'path': path,
            'depth': depth,
            'server': hostname
        })
        
    data = pd.DataFrame(data)
    with lock:
        data.to_csv(csv, mode='a', index=False, header=False)


def main():
    
    
    if not os.path.exists(csv):
        with open(csv, 'w+') as f:
            f.write('size,path,depth,host\n')
            

    pattern1 = re.compile('^/data[a-z0-9]*$')
    pattern2 = re.compile('^/tank$')

    disks = [i.mountpoint for i in psutil.disk_partitions()]
    disks = [i for i in disks if pattern1.match(i) or pattern2.match(i)]
    print('Analyzing ' + str(disks))
    
    threads = [threading.Thread(target=calc_usage, args=[i]) for i in disks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    
    


if __name__ == '__main__':
    
    main()

