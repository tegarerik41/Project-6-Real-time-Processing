#!python3

import os
import json
import pandas

from modelling import FraudModel
from kafka import KafkaConsumer
from sqlalchemy import create_engine

def transformStream(df):
    df = df \
            .groupby(['Id','devideId','logActivity']) \
            .agg({'logTimestamp':['sum','count']}) \
            .reset_index()
    df.columns = ['Id','newbalanceDest','device', 'timeformat1', 'timeformat2']
    
    return df.head(1)

if __name__ == "__main__":
    path = os.getcwd()+"\\"
    engine = create_engine('postgresql://postgres:admin@localhost:5432/digitalskola')
    consumer = KafkaConsumer("digitalskola", bootstrap_servers='127.0.0.1')
    print("starting the consumer")
    
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")
        
        try:
            df = pandas.read_sql_query(f"""
                                        select * 
                                        from user_activity 
                                        where user_activity."Id" = {data['Id']};""", engine)
        except:
            pass
            
        pandas.DataFrame(data, index=[0]) \
            .to_sql('user_activity', engine, if_exists='append', index=False)

        if df.shape[0] == 0:
            pass
        else:
            status = FraudModel.runModel(transformStream(df), path)
            print(f"User Predict: {status}")

        pandas \
            .DataFrame({'userId':[data['Id']], 'userFlag':[status]})  \
            .to_sql('user_fraud',  engine, if_exists='append', index=False)