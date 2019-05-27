import os
from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(30)
def func():
    os.system('python client2.py')

for i in range(10):
    executor.submit(func)