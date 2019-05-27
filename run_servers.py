
import os
from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(30)
def func(global_ip,local_ip,port):
    os.system('python server.py '+global_ip+' '+local_ip+' '+str(port))

f = open("config", "r")
info=[]
for x in f:
    if x[-1]=="\n":
        info.append(x[:-1])
    else:
        info.append(x)

# thread1 = executor.submit(func, 5000)
# thread2 = executor.submit(func, 5001)
# thread3 = executor.submit(func, 5002)

for i in range(1,len(info)):
    to_contact = info[i].split(":")
    executor.submit(func, to_contact[0],to_contact[1],to_contact[2])



