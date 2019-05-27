import numpy as np
import socket
from multiprocessing import Pool
import multiprocessing.pool
from collections import Counter
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from random import randint
import random
import threading
import time
class twophasecommit(object):
    def __init__(self,config,nb_replicas,pool,f):

        self.config=config
        self.connections={}
        self.nb_replicas=nb_replicas

        self.nbnodes = (len(self.config) - 1) / self.nb_replicas

        self.additional_connections = {}
        self.pool=pool
        self.log=f

        self.file_lock = threading.Lock()


    # def handle_get(self,operation):
    #
    #     splitted = operation.split(" ")
    #
    #     key = splitted[1]
    #     node = np.zeros(self.nb_replicas, dtype=np.int32)
    #     for i in range(self.nb_replicas):
    #         node[i] = int(key) % self.nbnodes + 1 + int(i * self.nbnodes)
    #
    #     s = {}
    #     for i in range(self.nb_replicas):
    #         ipandport = self.config[int(node[i])].split(":")
    #         host = ipandport[0]
    #         port = ipandport[2]
    #
    #         if node[i] not in self.connections:
    #
    #             s[node[i]] = (socket.create_connection((host, int(port))), operation, key)
    #
    #             self.connections[node[i]] = s[node[i]][0]
    #
    #         else:
    #             s[node[i]] = self.connections[node[i]]
    #             s[node[i]] = (self.connections[node[i]], operation, key)
    #
    #     input_for_parallel = s.values()
    #
    #     successful_ack = 0
    #
    #     #random.shuffle(input_for_parallel)
    #
    #     while not successful_ack:
    #         positive_ack = []
    #         result_prepare = map(self.prepare_phase, input_for_parallel)
    #         for i, response in enumerate(result_prepare):
    #             if response == "TRUE":
    #                 positive_ack.append(input_for_parallel[i])
    #
    #         if len(positive_ack) < len(input_for_parallel):
    #             result_abort = map(self.abort, positive_ack)
    #         else:
    #             successful_ack = 1
    #
    #     result = map(self.do_get, input_for_parallel)
    #
    #     #check_all_updated = sum([int(res == 'TRUE') for res in result]) == len(input_for_parallel)
    #     data=result[0]
    #
    #     return data

    def do_get(self,s):
        s[0].send(s[1].encode('ascii'))
        data = s[0].recv(1024)
        response = str(data.decode('ascii'))
        return response

    def handle_get(self,operation):

        splitted = operation.split(" ")

        key = splitted[1]
        node = np.zeros(self.nb_replicas, dtype=np.int32)
        for i in range(self.nb_replicas):
            node[i] = int(key) % self.nbnodes + 1 + int(i * self.nbnodes)

        chosen_replica_to_read_from = randint(0, self.nb_replicas-1)
        ipandport = self.config[int(node[chosen_replica_to_read_from])].split(":")
        host = ipandport[0]
        port = ipandport[2]

        if node[chosen_replica_to_read_from] not in self.connections:

            conn = socket.create_connection((host, int(port)))
            self.connections[node[chosen_replica_to_read_from]] = conn

        else:
            conn = self.connections[node[chosen_replica_to_read_from]]

        while True:
            # message sent to server
            conn.send(operation.encode('ascii'))

            # messaga received from server
            data = conn.recv(1024)
            break

        return data


    def handle_3puts(self,operation):

        splitted = operation.split(" ")
        keys=[splitted[1],splitted[3],splitted[5]]
        values = [splitted[2], splitted[4], splitted[6]]

        counter_of_nodes=np.zeros(int(self.nbnodes)*self.nb_replicas+1)

        node = np.zeros((len(keys), self.nb_replicas), dtype=np.int32)
        s = {}
        for i in range(len(keys)):
            for j in range(self.nb_replicas):
                node[i][j] = int(keys[i]) % self.nbnodes + 1 + int(j * self.nbnodes)
                ipandport = self.config[int(node[i][j])].split(":")
                host = ipandport[0]
                port = ipandport[2]

                counter_of_nodes[node[i][j]]+=1

                if counter_of_nodes[node[i][j]]==1:

                    if node[i][j] not in self.connections:

                        s[node[i][j]] = (socket.create_connection((host, int(port))), "PUT" + " " + str(keys[i]) + " " + values[i], keys[i],node[i][j])
                        self.connections[node[i][j]] = s[node[i][j]][0]

                    else:
                        s[node[i][j]] = (self.connections[node[i][j]],"PUT"+" "+str(keys[i])+" "+values[i],keys[i],node[i][j])

                else:
                    id=str(node[i][j])+'_'+str(counter_of_nodes[node[i][j]])
                    if id not in self.additional_connections:

                        s[id] = (socket.create_connection((host, int(port))), "PUT" + " " + str(keys[i]) + " " + values[i],keys[i],node[i][j])
                        self.additional_connections[id] = s[id][0]

                    else:
                        s[id] = (self.additional_connections[id], "PUT" + " " + str(keys[i]) + " " + values[i], keys[i],node[i][j])



        input_for_parallel = s.values()

        successful_ack = 0



        while not successful_ack:
            random.shuffle(input_for_parallel)
            positive_ack=[]
            result_prepare = self.pool.map(self.prepare_phase, input_for_parallel)
            for i,response in enumerate(result_prepare):
                if response=="TRUE":
                    positive_ack.append(input_for_parallel[i])



            if len(positive_ack)<len(input_for_parallel):
                result_abort = self.pool.map(self.abort, positive_ack)
                time.sleep(random.uniform(0, 3))
            else:
                successful_ack=1


        result=self.pool.map(self.do_put, input_for_parallel)

        check_all_updated=sum([int(res=='TRUE') for res in result])==len(input_for_parallel)
        return check_all_updated


    def handle_put(self,operation):


        original_put_operation=operation
        splitted=operation.split(" ")


        key=splitted[1]
        node=np.zeros(self.nb_replicas,dtype=np.int32)
        for i in range(self.nb_replicas):
            node[i] = int(key) % self.nbnodes + 1+int(i*self.nbnodes)

        s={}
        for i in range(self.nb_replicas):
            ipandport = self.config[int(node[i])].split(":")
            host = ipandport[0]
            port = ipandport[2]

            if node[i] not in self.connections:

                s[node[i]] = (socket.create_connection((host, int(port))),original_put_operation, key,node[i],self.log)

                self.connections[node[i]] = s[node[i]][0]

            else:
                s[node[i]] = self.connections[node[i]]
                s[node[i]] = (self.connections[node[i]],original_put_operation, key,node[i],self.log)

        input_for_parallel = s.values()

        successful_ack=0



        while not successful_ack:
            random.shuffle(input_for_parallel)
            self.log.write("start prepare of,"+original_put_operation+",\n")
            positive_ack=[]
            result_prepare = self.pool.map(self.prepare_phase_with_log, input_for_parallel)
            for i,response in enumerate(result_prepare):
                if response=="TRUE":
                    positive_ack.append(input_for_parallel[i])



            if len(positive_ack)<len(input_for_parallel):
                self.log.write("result of voting for," + original_put_operation + ",is negative\n")
                self.log.write("start abort for," + original_put_operation+",\n")
                result_abort = self.pool.map(self.abort_with_log, positive_ack)
                time.sleep(random.uniform(0, 3))
            else:
                successful_ack=1
                self.log.write("result of voting for," + original_put_operation + ",is positive\n")


        #result=self.pool.imap_unordered(self.do_put, input_for_parallel)
        for r in self.pool.imap_unordered(self.do_put_with_log, input_for_parallel):
            self.log.write("result of commit for," + original_put_operation + ",is," + r + ",\n")

        #check_all_updated=sum([int(res=='TRUE') for res in result])==len(input_for_parallel)
        return True


    def do_put(self,s):
        s[0].send(s[1].encode('ascii'))
        data = s[0].recv(1024)
        response = str(data.decode('ascii'))
        response=response+" from "+str(s[3])
        return response

    def prepare_phase(self,s):
        operation = "LOCK" + " " + str(s[2])
        while True:
            # message sent to server
            s[0].send(operation.encode('ascii'))

            # messaga received from server
            data = s[0].recv(1024)
            break

        return data

    def prepare_phase_with_log(self,s):
        operation = "LOCK" + " " + str(s[2])
        while True:
            # message sent to server
            s[0].send(operation.encode('ascii'))
            self.file_lock.acquire()
            s[4].write("prepare for," + s[1] + ",to node," + str(s[3]) + ",is sent" + "\n")
            self.file_lock.release()

            # messaga received from server
            data = s[0].recv(1024)
            break

        self.file_lock.acquire()
        s[4].write("result of prepare for," + s[1] + ",in node,"+ str(s[3])+",is,"+data+",\n")
        self.file_lock.release()

        return data

    def do_put_with_log(self,s):
        s[0].send(s[1].encode('ascii'))
        self.file_lock.acquire()
        s[4].write("commit for," + s[1] + ",to node," + str(s[3]) + ",is sent"+"\n")
        self.file_lock.release()
        data = s[0].recv(1024)
        response = str(data.decode('ascii'))
        response=response+" from "+str(s[3])
        return response


    def abort_with_log(self, s):
        operation = "RELEASE" + " " + str(s[2])
        #print(operation)
        while True:
            # message sent to server
            s[0].send(operation.encode('ascii'))
            self.file_lock.acquire()
            s[4].write("abort for," + s[1] + ",to node," + str(s[3]) + ",is sent" + "\n")
            self.file_lock.release()

            # messaga received from server
            data = s[0].recv(1024)
            break

        self.file_lock.acquire()
        s[4].write("result of abort for," + s[1] + ",in node," + str(s[3]) + ",is," + data + ",\n")
        self.file_lock.release()
        return data

    def abort(self, s):
        operation = "RELEASE" + " " + str(s[2])
        #print(operation)
        while True:
            # message sent to server
            s[0].send(operation.encode('ascii'))

            # messaga received from server
            data = s[0].recv(1024)
            break
        return data


    def check_servers(self):

        not_success=1
        while (not_success):
            for i in range(1,len(self.config)):
                ipandport = self.config[i].split(":")
                host = ipandport[0]
                port = ipandport[2]



                try:
                    if i not in self.connections:
                        s=socket.create_connection((host, int(port)))
                        self.connections[i] = s
                    # else:
                    #     s=self.connections[i]
                    #     to_send="check"
                    #     s.send(to_send.encode('ascii'))
                    #     data = s.recv(1024)


                except Exception as e:
                    print('waiting for server to connect')

            if len(self.connections.values())==len(self.config)-1:
                not_success=0

        return True



