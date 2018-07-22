
'''

This file implements following case studies:

(1) task status prediction based on task events data
(2) task status prediction based on task usage data
(3) task status prediction based on task constraints data

As a demo, all the studies are conducted based on just one batch.

we define each of the 500 files as one batch. 

Author: Ping Zhang

'''

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
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
    # RandomForestClassifier --> RF
    trainingY = np.ravel(trainingY)
    RF = RandomForestClassifier(n_estimators=10)
    
    print('start training!')
    trainedRF = RF.fit(trainingX, trainingY)
    print('finish training!')

    featureImportances = trainedRF.feature_importances_

    return trainedRF, featureImportances   


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
    trainedRF, FeatureImportances = trainModel(traX, traY)

    PRECISION, RECALL = testModel(trainedRF, tesX, tesY)

    print('precision is %f'%(PRECISION))
    print('recall is %f'%(RECALL))
    print('feature importances are : ')
    print(FeatureImportances)





   
