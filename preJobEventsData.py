'''
----------------------------------------------------------
This file preprocesses job_events datasets:

for 500 datasets:

1. Add field names of 8 columns
2. Map string hash of user to----------------- UserId
3. Map string hash of job name to------------- NameId
4. Map string hash of logical job name to-- LogNameId

then, writes them out to local disk.

#job_events:   8 fields --> JE

Author: Ping Zhang

----------------------------------------------------------
'''

print(__doc__)

import pandas as pd
import numpy as np
from threading import Thread
import os 

userNameStr = pd.read_csv('userNameStr',header=None)
jobNameStr = pd.read_csv('jobNameStr',header=None)
logicalJobNameStr = pd.read_csv('logicalJobNameStr',header=None)

userID = np.arange(1,len(userNameStr)+1)
nameID = np.arange(1,len(jobNameStr)+1)
logNameID = np.arange(1,len(logicalJobNameStr)+1)

replaceValue_user, replaceValue_name, replaceValue_log = {}, {}, {}

for user, ID1 in zip(userNameStr[0].tolist(), userID):
    replaceValue_user[user] = ID1

for job, ID2 in zip(jobNameStr[0].tolist(), nameID):
    replaceValue_name[job] = ID2

for logi, ID3 in zip(logicalJobNameStr[0].tolist(),logNameID):
    replaceValue_log[logi] = ID3

JE_fieldNames = ['TimeStamp', 'MissInfo', 'JobId', 'EventType', 
                 'UserId', 'SchedulClass','NameID','LogNameID']

def RefineJE(JE, FILE):
    try:
        JE = JE.replace({4:replaceValue_user})
    except:
        print('failed to map user name to userID!')

    try:
        JE = JE.replace({6:replaceValue_name})
    except:
        print('failed to map job name to nameID!')
    
    try:
        JE = JE.replace({7:replaceValue_log})
    except:
        print('failed to map logical job name to LogNameID!')


    JE.to_csv('../job_events/%s'%(FILE),header=JE_fieldNames,index=None)


if __name__ == '__main__':
    
    for root, _, fileList in os.walk('../job_events/'):
        for FILE in fileList:
            filePath = os.path.join(root, FILE)
            print('refining %s'%(FILE))
            JE = pd.read_csv(filePath,header=None)
            RefineJE(JE, FILE)


