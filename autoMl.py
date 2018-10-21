
'''

This file implements following case studies:

try the auto machine leanrning based on the sklearn

we define each of the 500 files as one batch. 

Author: Ping Zhang

'''

import pandas as pd
import numpy as np
import autosklearn.classification
from sklearn import metrics


global SplitRatio    # ratio between training set and test set
global SampleRatio   # ratio used to sample data recodes on 500 batches(files)


def prepareDataSets():
    TE = pd.read_csv('RefinedTaskEventBatchData/refined-part-00000-of-00500.csv')
    
    rows = len(TE)
    SplitRatio = 0.8
    
    TrainSet = TE.sample(n=int(rows*SplitRatio))
    TestIndex = TE.index.difference(TrainSet.index)
    TestSet = TE.iloc[TestIndex]
    
    FeatureNames = ['UserId', 'SchedulClass', 'Priority', 'ReqCPUCores',
                    'ReqRAM', 'ReqDisk', 'DiffMachineCons']
    
    TargetName = ['EventType']

    trainX = TrainSet[FeatureNames]
    trainY = TrainSet[TargetName]
    
    testX = TestSet[FeatureNames]
    testY = TestSet[TargetName]
    
    # convert data from pd.DataFrame to np.array
    trainX = trainX.values
    trainY = trainY.values
    testX = testX.values
    testY = testY.values

    return trainX, trainY, testX, testY 


def trainModel(trainingX, trainingY):
    trainingY = np.ravel(trainingY)
    autoCls = autosklearn.classification.AutoSklearnClassifier() 
    
    print('start training!')
    trainedAutoCls = autoCls.fit(trainingX, trainingY)
    print('finish training!')

    return trainedAutoCls


def precision(y_true, y_pred):
    p = metrics.precision_score(y_true, y_pred, average='macro')

    return p


def recall(y_true, y_pred):
    r = metrics.recall_score(y_true, y_pred, average='micro')

    return r


def testModel(model, testingX, testingY):
    y_prediction = model.predict(testingX)

    pp = precision(testingY, y_prediction)
    rr = recall(testingY, y_prediction)

    return pp, rr



if __name__ == '__main__':

    traX, traY, tesX, tesY = prepareDataSets()
    trainedAutoCls = trainModel(traX, traY)

    PRECISION, RECALL = testModel(trainedAutoCls, tesX, tesY)

    print('precision is %f'%(PRECISION))
    print('recall is %f'%(RECALL))





   
