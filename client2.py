# Import socket module
import socket
from random import randint
import string
import random
import time

import numpy as np

import TwoPhaseCommit1
from multiprocessing import Process, Lock

import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.dummy import Pool as ThreadPool

executor = ThreadPoolExecutor(30)

def Main(thread):
    f = open("config", "r")
    info=[]
    for x in f:
        if x[-1]=="\n":
            info.append(x[:-1])
        else:
            info.append(x)

    range1=info[0].split("-")
    min=range1[0]
    max=range1[1]
    thread=int(thread)
    nboperations=1000
    nb_unsuccessful_get=0
    nb_successful_get=0
    nb_unsuccessful_put=0
    nb_successful_put=0

    nb_replicas=2
    pool = ThreadPool(nb_replicas*3)
    f = open("log"+str(thread)+".txt", "w")
    tpc=TwoPhaseCommit1.twophasecommit(info,nb_replicas,pool,f)

    tpc.check_servers()
    start = time.time()

    for npp in range(nboperations):
        
        r=random.random()
        if (r<=0.6):
            
            key = randint(int(min), int(max))
            
            op="GET"
            
            operation="GET"+" "+str(key)
            
            data = tpc.handle_get(operation)

        elif (r>0.6 and r<=0.8):
            
            key = randint(int(min), int(max))
            
            value = ''.join(
            random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(2))

            op = "PUT"
            
            operation="PUT"+" "+str(key)+" "+value
            
            data=tpc.handle_put(operation)

        else:
            nb_keys=3
            values=[None]*nb_keys


            
            keys = np.random.permutation(int(max) + 1)[0:3]
            
            for i in range(0,nb_keys):
                values[i] = ''.join(
                    random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(2))

            op = "PUT3"
            operation = "PUT3"
            for i in range(nb_keys):
                operation=operation+" "+str(keys[i]) + " " + values[i]
            
            data=tpc.handle_3puts(operation)


        response=str(data)
        #print('Received from the server :', response)



    end = time.time()
    period=end-start


    print ('the system executes {} operations in {} seconds'.format(nboperations,period))

if __name__ == '__main__':
    number_of_threads=6
    # for i in range(number_of_threads):
    #     executor.submit(Main,i)

    for i in range(number_of_threads):
        Process(target=Main, args=(i,)).start()

    
