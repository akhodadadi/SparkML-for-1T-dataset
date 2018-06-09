from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions
from time import ctime

def create_dataproc_cluster(dataproc,projectId,clusterName,region,zone,
                            masterNumInst=1,masterMachineType='n1-standard-1',
                            workerNumInst=2,workerMachineType='n1-standard-1',
                            bootDiskSizeGb=10,preemptibleWorkers=False):
    
    '''
    This function creates a GCP dataproc cluster.
    
    Parameters
    ----------
    dataproc: dataproc client
        get this by:
        >>> dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    projectId: str
    clusterName: str
    region: str
    zone: str
    masterNumInst: int
        number of master machines
    masterMachineType: str
        mster machine types, for example 'n1-standard-2'.
    bootDiskSizeGb: int
        Primary disk size (minimum 10 GB)        
    '''
    
    zone_uri = \
        'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
            projectId, zone)
    gceClusterConfig={'zoneUri': zone_uri}
    diskConfig={'bootDiskSizeGb':bootDiskSizeGb}
    
    masterConfig={'numInstances': masterNumInst,
                  'machineTypeUri': masterMachineType,
                  'diskConfig':diskConfig,
                  'isPreemptible':False}
    
    workerConfig={'numInstances': workerNumInst,
                  'machineTypeUri': workerMachineType,
                  'diskConfig':diskConfig,
                  'isPreemptible':preemptibleWorkers}
    
    
    cluster_config={'gceClusterConfig':gceClusterConfig,
                    'masterConfig':masterConfig,
                    'workerConfig':workerConfig}
    
    cluster_data={'projectId': projectId,
                  'clusterName': clusterName,
                  'config':cluster_config}
    
    result = dataproc.projects().regions().clusters().create(
            projectId=projectId,region=region,body=cluster_data).execute()
    return result
    
    
def upload_loca_files_to_bucket(srcDir,bucketName,fileNames):
    '''
    This function uploads files in a directory to a bucket on storage.
    
    Parameters
    ---------
    srcDir: str
        source directory 
    bucketName: str
        name of the destination bucket
    fileNames: list 
        list of name of files to be uploaded.
    '''
    
    storage_client = storage.Client()
    
    #===create or get the bucket===
    bucket = storage_client.lookup_bucket(bucketName)
    if bucket is None:
        bucket = storage.Bucket(storage_client,name=bucketName)
        bucket.location='us-east4'
        bucket.create()
    #===create or get the bucket===
    
    #===create a blob===
    for fn in fileNames:
        blobName=fn
        blob = bucket.blob(blobName)
        blob.upload_from_filename(fn)
    #===create a blob===
    
def load_data_from_storage_to_bigquery(dataset_id,table_id,schema,
                                       delimiter=','):
    '''
    This function loads data from csv file on a storage bucket 
    to a bigquery table.
    
    Parameters
    ---------
    dataset_id: str
    table_id: str
    schema: a list of bigquery.SchemaField. Example:
        >>> schema=[bigquery.SchemaField(name='col_{}'.format(i),
                                 field_type='STRING',mode='NULLABLE')\
                    for i in range(14,40)]
    delimiter: 
        delimiter for the csv file.
    '''
    bq_client = bigquery.Client()
    
    #===create or get dataset===
    dataset_ref=bq_client.dataset(dataset_id)
    try:
        dataset=bq_client.get_dataset(dataset_ref)
    except exceptions.NotFound:
        dataset=bigquery.Dataset(dataset_ref)    
        dataset = bq_client.create_dataset(dataset)
    #===create or get dataset===
    
    #===create or get table===
    tab_ref=dataset.table(table_id)
    try:
        table = bq_client.get_table(tab_ref)
    except exceptions.NotFound:
        table=bigquery.Table(tab_ref,schema=schema)
        bq_client.create_table(table)
    #===create or get table===
    
    #===load data from cloud storage to table===
    print(ctime()+'...loading data from storage to table: {}'.\
          format(tab_ref.table_id))
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter=delimiter
    job_config.schema=schema
    job_config.write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    
    load_job = bq_client.load_table_from_uri('gs://criteo_small/day_0',
                                          destination=tab_ref,
                                          job_config=job_config)
    
    load_job.result()# Waits for table load to complete.
    load_job.state
    print(ctime()+'...Job status: {}'.format(load_job.state))
    #===load data from cloud storage to table===
    
    
    
    
    
    
    
    