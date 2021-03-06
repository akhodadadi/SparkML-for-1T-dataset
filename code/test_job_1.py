from criteo import criteo_config
from pyspark.sql import SparkSession
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.classification import LogisticRegression
spark=SparkSession.builder.getOrCreate()
from time import time,ctime

numFeatures=2**18
regParam=.01
elasticNetParam=0.
maxIter=50

data_bucket_name='criteo-database'
blob_name='day_0_med'
data_uri='gs://{}/{}'.format(data_bucket_name, blob_name)

results_bucket_name='criteo-results'
fittedModel_uri='gs://{}/{}'.format(results_bucket_name, 'fitted_model')
eval_uri='gs://{}/{}'.format(results_bucket_name, 'results')

df=spark.read.csv(path=data_uri,sep='\t',header=False,
                  schema=criteo_config.TABLE_SCHEMA)


#===hash features===
hasher=FeatureHasher(inputCols=df.columns,outputCol='features',
                     numFeatures=numFeatures)
df=hasher.transform(df)
#===hash features===

#===split train and validation===
train, test = df.randomSplit([0.9, 0.1], seed=12345)
#===split train and validation===

#===fit logestic regression===
#elasticNetParam=1 -->l1 regularization
startTime=time()
lr = LogisticRegression(maxIter=maxIter, regParam=regParam,
                        elasticNetParam=elasticNetParam)
lrModel = lr.fit(train)
#lrModel.save(fittedModel_uri)
trainTime=time()-startTime
#===fit logestic regression===

#===evaluate on test===
startTime=time()
evaluate = lrModel.evaluate(test)
print(ctime()+'...evaluating on test set...')
auc=evaluate.areaUnderROC
acc=evaluate.accuracy
numFeatures=hasher.getNumFeatures()
print(ctime()+'...counting number of samples...')
n_samples=df.count()
evalTime=time()-startTime

results=spark.createDataFrame([(numFeatures,n_samples,auc,acc,trainTime,
                                evalTime,regParam,elasticNetParam,maxIter)]).\
        toDF('numFeatures','n_samples','AUC','accuracy','trainTime',
             'evalTime','regParam','elasticNetParam','maxIter')

print(ctime()+'...saving the results...')        
results.write.csv(eval_uri,header=True,mode='append')
#===evaluate on test===
