from pyspark.sql import SparkSession
from criteo import criteo_config
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.classification import LogisticRegression
from time import time
spark=SparkSession.builder.getOrCreate()

numFeatures=2**18
regParam=.01
elasticNetParam=0.
maxIter=50

#===load data into sql datafram===
fn='/home/arash/datasets/Criteo/day_0_tiny'
df=spark.read.csv(fn,sep='\t',header=False,schema=criteo_config.TABLE_SCHEMA)
#===load data into sql datafram===

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
trainTime=time()-startTime
#===fit logestic regression===

#===evaluate on test===
startTime=time()
evaluate = lrModel.evaluate(test)
auc=evaluate.areaUnderROC
acc=evaluate.accuracy
numFeatures=hasher.getNumFeatures()
n_samples=df.count()
evalTime=time()-startTime

results=spark.createDataFrame([(numFeatures,n_samples,auc,acc,trainTime,
                                evalTime,regParam,elasticNetParam,maxIter)]).\
        toDF('numFeatures','n_samples','AUC','accuracy','trainTime',
             'evalTime','regParam','elasticNetParam','maxIter')
    
results.write.csv('/home/arash/datasets/Criteo/results',header=True,
                  mode='append')
#===evaluate on test===
