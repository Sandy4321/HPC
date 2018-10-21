'''
-----------------------------------------------------------
This file sorts task_events datasets by dictionary order:
    {'JobId', 'TaskIdInThisJob', 'TimeStamp'}

Author: Ping Zhang
-----------------------------------------------------------

'''

print(__doc__)

import pandas as pd
from multiprocessing import Pool
import os
import time   

sort_dictionary = ['JobId', 'TaskIdInThisJob', 'TimeStamp']

def Sorting(TE, FILE):
    TE.sort_values(by=sort_dictionary, inplace=True)
    TE.to_csv('../sorted_task_events/%s'%(FILE),index=None)

def handle(filePath, FILE):
    print('processing ... %s'%(FILE))
    TE = pd.read_csv(filePath)
    Sorting(TE, FILE)


if __name__ == '__main__':
   t1 = time.time()
   p = Pool()
   for root, _, fileList in os.walk('../task_events/'):
       for FILE in fileList:
           filePath = os.path.join(root, FILE)
           p.apply_async(handle, args=(filePath, FILE, ))
   p.close()
   p.join()
   t2 = time.time()
   print('time cost is ... %f'%(t2-t1))




