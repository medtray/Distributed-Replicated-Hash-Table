import threading
import socket
import sys
import HashTable2
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import itertools


dht = HashTable2.MyHashMap(200000)
#hash_key = hash('0') % dht.size_HM
#dht.lock[hash_key].acquire()
counter = itertools.count()
period=3


import time, threading



def handle(connection, address,end_interval,log,file_lock):



    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("process-%r" % (address,))

    try:
        logger.debug("Connected %r at %r", connection, address)


        while True:
            data = connection.recv(1024)
            if data.decode() == "":
                logger.debug("Socket closed remotely")
                break
            logger.debug("Received data %r", data)

            splitted=data.decode().split(" ")
            if splitted[0]=="GET":
                result=dht.get(splitted[1])
                if time.time()<=end_interval:
                    next(counter)
                #logger.debug("server handles %r operations in %r seconds", str(counter),str(period))
                if result==None:
                    to_send="NONE"
                else:
                    to_send=str(result)
                connection.sendall(to_send.encode())

            if splitted[0]=="PUT":
                result=dht.put(splitted[1],splitted[2])
                if time.time()<=end_interval:
                    next(counter)
                #logger.debug("server handles %r operations in %r seconds", str(counter),str(period))
                file_lock.acquire()
                log.write("commit for," + data +",from,"+str(connection)+","+str(address)+",is,"+str(result)+ "\n")
                file_lock.release()
                if (result):
                    to_send="TRUE"
                else:
                    to_send="FALSE"

                connection.sendall(to_send.encode())

            if splitted[0]=="LOCK":
                result=dht.try_to_lock(splitted[1].encode())
                file_lock.acquire()
                log.write("prepare for," + data + ",from," + str(connection) + "," + str(address) + ",is," + str(
                    result) + "\n")
                file_lock.release()
                if (result):
                    to_send="TRUE"
                else:
                    to_send="FALSE"

                connection.sendall(to_send.encode())

            if splitted[0]=="RELEASE":
                result=dht.release_lock(splitted[1].encode())
                file_lock.acquire()
                log.write("abort for," + data + ",from," + str(connection) + "," + str(address) + ",is," + str(
                    result) + "\n")
                file_lock.release()
                if (result):
                    to_send="TRUE"
                else:
                    to_send="FALSE"

                connection.sendall(to_send.encode())

            if splitted[0]=="check":
                to_send="OK"
                connection.sendall(to_send.encode())

            logger.debug("Sent data")
    except:
        logger.exception("Problem handling request")
    finally:
        logger.debug("Closing socket")
        #logger.debug("server handles %r operations in %r seconds", str(counter),str(period))

        #connection.close()

class Server(object):

    def __init__(self, hostname, port,globalIP):
        import logging
        self.logger = logging.getLogger("server")
        self.hostname = hostname
        self.port = port
        self.globalIP = globalIP
        self.timer_started=0
        self.log = open("log_server"+".txt", "w")

        self.file_lock = threading.Lock()



    def start(self):
        import logging
        logging.basicConfig(level=logging.DEBUG)

        def show_counter():
            logging.info("server handles %r operations in %r seconds" , str(counter), str(period))
            threading.Timer(3, show_counter).start()

        executor = ThreadPoolExecutor(100)
        self.logger.debug("listening")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.hostname, self.port))
        self.socket.listen(100)



        show_counter()
        while True:
            conn, address = self.socket.accept()
            self.logger.debug("Got connection")
            #logging.info("server handles %r operations in %r seconds" , str(counter), str(period))

            #thread = threading.Thread(target=handle, args=(conn, address,self.globalIP,self.port))
            if not self.timer_started:
                self.timer_started=1
                start_interval=time.time()
                end_interval=start_interval+period
            thread = executor.submit(handle, conn, address,end_interval,self.log,self.file_lock)
            #process.daemon = True
            #thread.start()
            self.logger.debug("Started process %r", thread)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)

    globalIP=sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]

    server = Server(ip, int(port),globalIP)


    try:
        logging.info("Listening")
        server.start()
    except:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Shutting down")
        #logging.info("server handles %r operations in %r seconds", str(counter),str(period))


