
import os
from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(30)
def func(name):

    os.system('ssh -i "mykey.pem" '+name+ ' \'cd /home/ec2-user/RHT && python < client2.py'+'\'')

    


f = open("names", "r")
names=[]
for x in f:
    if x[-1]=="\n":
        names.append(x[:-1])
    else:
        names.append(x)


for i in range(len(names)):
    executor.submit(func,names[i])



