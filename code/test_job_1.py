from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
spark=SparkSession.builder.getOrCreate()

data_bucket_name='criteo_small'
blob_name='day_0'
data_uri='gs://{}/{}'.format(data_bucket_name, blob_name)

results_bucket_name='criteo-results'
results_uri='gs://{}/{}'.format(results_bucket_name, 'results')

df=spark.read.csv(path=data_uri,sep='\t',header=False)
df=df.select(F.countDistinct('_c0')).toDF('N')
df.write.csv(results_uri)