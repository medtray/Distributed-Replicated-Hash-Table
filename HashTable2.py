import threading
import time

class MyHashMap(object):
    def __init__(self,size_HM):
        """
        Initialize your data structure here.
        """
        self.items = [[] for _ in range(size_HM)]
        self.lock = [threading.Lock() for _ in range(size_HM)]
        self.size_HM=size_HM

    def put(self, key, value):
        hash_key = int(key) % self.size_HM
        #self.lock[hash_key].acquire()
        #time.sleep(50)
        bucket = self.items[hash_key]

        for num,kv in enumerate(bucket):
            k, v = kv
            if key == k:
                (bucket[num])[1]=value
                self.lock[hash_key].release()
                return True

        bucket.append([key, value])
        self.lock[hash_key].release()
        return True

    def get(self, key):
        hash_key = int(key) % self.size_HM
        self.lock[hash_key].acquire()
        bucket = self.items[hash_key]
        for kv in bucket:
            k, v = kv
            if key == k:
                self.lock[hash_key].release()
                return v

        self.lock[hash_key].release()
        return None


    def try_to_lock(self,key):
        hash_key = int(key) % self.size_HM
        if self.lock[hash_key].locked():
            return False
        else:

            return self.lock[hash_key].acquire()

    def release_lock(self,key):
        hash_key = int(key) % self.size_HM
        #if self.lock[hash_key].locked():
        self.lock[hash_key].release()

        return True



