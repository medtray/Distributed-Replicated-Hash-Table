
import os
from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(30)
def func(global_ip,local_ip,port,name):

    os.system('ssh -i "mykey.pem" '+name+ ' \'sudo fuser -k '+str(port)+'/tcp'+'\'')




f = open("config", "r")
info=[]
for x in f:
    if x[-1]=="\n":
        info.append(x[:-1])
    else:
        info.append(x)

f = open("names", "r")
names=[]
for x in f:
    if x[-1]=="\n":
        names.append(x[:-1])
    else:
        names.append(x)


for i in range(1,len(info)):
    to_contact = info[i].split(":")
    executor.submit(func, to_contact[0],to_contact[1],to_contact[2],names[i-1])



