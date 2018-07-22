
'''

This file preprocesses following datasets related with task in a parallel mode:

(1) task_events
(2) task_usage
(3) task_constraints

then, writes them out to local disk.

# task_events:      13 fields --> TE
# task_usage:       20 fields --> TU
# task_constraints: 6  fields --> TC

Author: Ping Zhang


'''

print(__doc__)

import pandas as pd
import numpy as np
from threading import Thread
import os 


def RefineTE(TE, file):
    ''' function for preprocessing task_events datasets '''
    
    # 1. extract recodes with 'missing info' field empty
    TE.loc[:,(1)].fillna(1000,inplace=True)
    TE = TE[TE[1] == 1000]
    try:
        TE = TE.drop([1],axis=1)
    except:
        print('failed to delete ''missing info'' field!')
    
    # 2. eliminate recodes with 'NaN' in any field
    try:
        TE.dropna(axis=0, how='any', inplace=True)
    except:
        print('failed to eliminate recodes with NaN!')
    
    # 3. filter the recodes of EventType 0, 1, 6, 7 and 8.
    TE = TE[(TE[5] > 1) & (TE[5] < 6)]

    # 4. transform the encoded strings in field 'user name' to user ID, starting from 1.
    userNameSet = set(TE[6].tolist())
    userID = np.arange(1,len(userNameSet)+1)
    replaceValue = {}
    for name, ID in zip(userNameSet, userID):
        replaceValue[name] = ID
    try:
        TE = TE.replace({6:replaceValue})
    except:
        print('failed to transform user name to userID!')
    
    print(len(TE))

    # 5. rename fields and write out to disk
    TE_fieldNames = ['TimeStamp', 'JobId', 'TaskIdInThisJob',
                     'MachineId', 'EventType', 'UserId', 
                     'SchedulClass', 'Priority', 'ReqCPUCores', 
                     'ReqRAM','ReqDisk','DiffMachineCons']

    TE.to_csv('RefinedTaskEventBatchData/refined-%s'%(file),\
        header=TE_fieldNames, index=None)


def RefineTU(TU):
    ''' function for preprocessing task_usage datasets '''

    # 1. eliminate recodes with 'NaN' in any field
    try:
        TU.dropna(axis=0, how='any', inplace=True)
    except:
        print('failed to eliminate recodes with NaN!')
    
    print(len(TU))

    # 2. rename fields and write out to disk
    TU_fieldNames = ['StartTime', 'EndTime', 'JobId', 'TaskIdInThisJob',
                    'MachineId', 'CPURate', 'CanoMemUsage', 'AssigMemUsage',
                    'UnmappedPageCache', 'TotalPageCache', 'MaxMemUsage',
                    'DiskIOTime','LocalDiskSpaceU', 'MaxCPURate', 'MaxDiskIOTime',
                    'CPI', 'MAI', 'SamplePortion', 'AggreType', 'SampledCPUUsage']
    
    TU.to_csv('TaskUsage',header=TU_fieldNames, index=None)
    

def RefineTC(TC):
    ''' function for preprocessing task_constraints datasets '''
    
    print(len(TC))

    # rename fields and write out to disk
    TC_fieldNames = ['TimeStamp', 'JobId', 'TaskIdInThisJob',
                     'CompOperator', 'AttribName', 'AttribValue']

    TC.to_csv('TaskConstraints',header=TC_fieldNames, index=None)


def handle(filePath, file):
    print('preprocessing ... %s' %(file))
    TE = pd.read_csv(filePath,header=None)
    RefineTE(TE, file)


class MyThread(Thread):
    def __init__(self, filePath, file):
        Thread.__init__(self)
        self.filePath = filePath
        self.file = file 

    def run(self):
        handle(self.filePath, self.file)


if __name__ == '__main__':

    threads = []

    for root, _, fileList in os.walk('TaskEventBatchData/'):
        for file in fileList:
            filePath = os.path.join(root, file) 
            thread = MyThread(filePath, file)
            threads.append(thread)
            thread.start()
            print('runing ... ')
        for t in threads:
            t.join()
    
    print('main thread')


