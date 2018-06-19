from gcp_utils import create_dataproc_cluster,upload_local_files_to_bucket,\
load_data_from_storage_to_bigquery,submit_pyspark_job,\
download_bucket_to_local_dir
import googleapiclient.discovery
from google.cloud import bigquery


#===what to do===
whatToDo=6

'''
1: load data to bigquery table
2: create a cluster
3: load data to storage 
4: load job files to storage 
5: submit job
6: download results to computer
'''    
#===what to do===


from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

projectId='sparkml-criteo-1t'
clusterName='test-cluster1'
region='us-east4';zone='us-east4-b'
#region='us-central1';zone='us-central1-a'


#===load data to bigquery table===
if whatToDo==1:
    dataset_id='criteo_small'
    table_id='day_0'
    
    #---table schema---
    labels_schema=bigquery.SchemaField(name='label',
                                       field_type='BOOL',mode='NULLABLE')
    num_schema=[bigquery.SchemaField(name='col_{}'.format(i),
                                     field_type='FLOAT',mode='NULLABLE')\
                for i in range(1,14)]
    
    cat_schema=[bigquery.SchemaField(name='col_{}'.format(i),
                                     field_type='STRING',mode='NULLABLE')\
                for i in range(14,40)]
    schema = [labels_schema]+num_schema+cat_schema
    #---table schema---
    
    load_data_from_storage_to_bigquery(dataset_id,table_id,schema,delimiter='\t')
#===load data to bigquery table===

#===create a cluster===
if whatToDo==2:
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')    
    d=create_dataproc_cluster(dataproc,projectId=projectId,
                              clusterName=clusterName,
                              region=region,zone=zone,
                              masterNumInst=1,workerNumInst=4,
                              masterMachineType='n1-standard-4',
                              workerMachineType='n1-standard-4',)
#===create a cluster===

#===load data to storage===
if whatToDo==3:    
    upload_local_files_to_bucket('/home/arash/datasets/Criteo',
                                'criteo-database',['day_0_med'])
#===load data to storage===

#===load job files to storage===
if whatToDo==4:    
    upload_local_files_to_bucket('/home/arash/GitHub/SparkML-for-1T-dataset/code',
                                'criteo-code',['test_job_1.py','criteo.zip'])
#===load job files to storage===

#===submit job===
if whatToDo==5:
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    d=submit_pyspark_job(dataproc,projectId=projectId,clusterName=clusterName,
                         code_bucket_name='criteo-code',region=region,
                         mainFilename='test_job_1.py',
                         otherFilesName='criteo.zip')
#===submit job===

#===download results to computer===
if whatToDo==6:
    download_bucket_to_local_dir('criteo-results',
                                 '/home/arash/datasets/Criteo')
    results=spark.read.csv('/home/arash/datasets/Criteo/results/',header=True)
    results=results.toPandas()
    print results
#===download results to computer===


