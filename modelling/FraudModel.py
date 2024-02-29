#!python3

import os
import pickle
import time

import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

from sklearn.linear_model import LogisticRegression

import warnings
warnings.filterwarnings('ignore')

def prepOneHotEncoder(df,col):
    labelOnetHotEncoder = OneHotEncoder(handle_unknown='ignore')
    dfOneHotEncoder = pd \
                        .DataFrame(labelOnetHotEncoder.fit_transform(df[[col]].values).toarray(), \
                                   columns = [col+"_"+str(i+1) for i in range(len(df[col].unique()))])
    
    filename = 'prep'+col+'.pkl'
    pickle \
        .dump(labelOnetHotEncoder, open(pathPackages+filename, 'wb'))
    print(f"Preprocessing data {col} has save...")
    
    df = pd \
            .concat([df.drop(col,axis=1), dfOneHotEncoder],axis=1)
    
    return df

def prepStandarScaler(df,col):
    scallingStandarScaler = StandardScaler()
    dfStandarScaler = scallingStandarScaler.fit_transform([df[col]])[0]

    filename = 'prep'+col+'.pkl'
    pickle \
        .dump(scallingStandarScaler, open(pathPackages+filename, 'wb'))
    print(f"Preprocessing data {col} has save...")
    
    df = df \
            .drop(col,axis=1)
    df[col] = dfStandarScaler
    
    return df

def runModel(data,path):
    path = path+"/"+"modelling"+"/"+"packages"+"/"
    col = pickle.load(open(path+'columnModelling.pkl', 'rb'))
    df = pd.DataFrame(data,index=[0])
    df = df[col]

    prepdevice = pickle.load(open(path+'prepdevice.pkl', 'rb'))
    dfDevice = pd \
                .DataFrame(prepdevice.transform([df['device']]).toarray(),
                          columns=["device_"+str(i+1) for i in range(len(prepdevice.transform([df['device']]).toarray()[0]))])
    df = pd.concat([df.drop('device',axis=1),dfDevice],axis=1)

    prepnewbalanceDest = pickle.load(open(path+'prepnewbalanceDest.pkl', 'rb'))
    prepnewbalanceDest = pd \
                .DataFrame(prepnewbalanceDest.transform([df['newbalanceDest']]).toarray(),
                          columns=["newbalanceDest_"+str(i+1) for i in range(len(prepnewbalanceDest.transform([df['newbalanceDest']]).toarray()[0]))])
    df = pd.concat([df.drop('newbalanceDest',axis=1),prepnewbalanceDest],axis=1)

    X = df.values.tolist()
    model = pickle.load(open(path+'modelFraud.pkl', 'rb'))
    y = model.predict(X)[0]
    if y == 0:
        return "Fraud"
    else:
        return "White List"

if __name__ == "__main__":
    pathPackages = os.getcwd()+"\\"+"packages"+"\\"
    target = 'isFraud'
    
    data = pd.read_csv(pathPackages+'Fraud.csv')
    df = data.drop(target,axis=1)
    pickle \
            .dump(df.columns.tolist(), open(pathPackages+'columnModelling.pkl', 'wb'))

    colOneHotEncoder = ['device','newbalanceDest']
    for col in colOneHotEncoder:
        df = prepOneHotEncoder(df,col)

    colprepStandarScaler = ['timeformat1']
    for col in colprepStandarScaler:
        df = prepStandarScaler(df,col)

    X = df.values.tolist()
    y = data[['isFraud']].values.tolist()
    
    start = time.time()
    model = LogisticRegression()
    model.fit(X,y)
    stop = time.time()
    
    with open(pathPackages+'modelFraud.pkl','wb') as file:
        pickle.dump(model, file)
    print(f"{stop-start} Training Model done create...")