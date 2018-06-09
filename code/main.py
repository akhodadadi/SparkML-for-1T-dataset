from gcp_utils import create_dataproc_cluster,upload_loca_files_to_bucket,
load_data_from_storage_to_bigquery

#===load data to bigquery table===
dataset_id='criteo_small'
table_id='day_0'

#---table schema---
labels_schema=bigquery.SchemaField(name='labels',
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
#import googleapiclient.discovery
#dataproc = googleapiclient.discovery.build('dataproc', 'v1')
#
#d=create_dataproc_cluster(dataproc,projectId='sparkml-criteo-1t',
#                          clusterName='test-cluster1',
#                          region='us-east4',zone='us-east4-a')
#===create a cluster===


#===load job files to storage===
upload_loca_files_to_bucket('/home/arash/GitHub/SparkML-for-1T-dataset/code',
                            'criteo-code',['test_job_1.py'])

#===load job files to storage===