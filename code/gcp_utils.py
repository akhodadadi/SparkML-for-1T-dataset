from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions
from time import ctime,time
import os
from os.path import join

def create_dataproc_cluster(dataproc,projectId,clusterName,region,zone,
                            masterNumInst=1,masterMachineType='n1-standard-1',
                            workerNumInst=2,workerMachineType='n1-standard-1',
                            bootDiskSizeGb=15,preemptibleWorkers=False):
    
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
    
    softwareConfig = {"imageVersion": "preview",
                      "properties":\
                          {'spark.executor.memoryOverhead':'4096'}
                      }
                #set to 'preview' to use pyspark 2.3
    
    cluster_config={'gceClusterConfig':gceClusterConfig,
                    'masterConfig':masterConfig,
                    'workerConfig':workerConfig,
                    "softwareConfig": softwareConfig}
                    
    
    cluster_data={'projectId': projectId,
                  'clusterName': clusterName,
                  'config':cluster_config}
    
    result = dataproc.projects().regions().clusters().create(
            projectId=projectId,region=region,body=cluster_data).execute()
    return result
    
    
def upload_local_files_to_bucket(srcDir,bucketName,fileNames):
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
    
    startTime=time()
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
        blob.upload_from_filename(join(srcDir,fn))
    #===create a blob===
    
    print('upload took {} seconds'.format(time()-startTime))
    
def download_bucket_to_local_dir(bucket_name,dst_dir):
    '''
    This function dowloads all objects in a bucket into `dst_dir`.
    
    Parameters
    ---------
    bucket_name: str
    dst_dir: str
        Destination directory on local computer where the contents of the 
        bucket will be saved.    
    '''
    
    storage_client=storage.Client()
    bucket=storage_client.get_bucket(bucket_name)#throughs an error if bucket
    #does not exist
    
    for blob in bucket.list_blobs():
        if blob.size==0:
            continue
        else:
            l=blob.name.split('/')
            for i in range(1,len(l)):
                path = '/'.join(l[0:i])
                if not os.path.exists(join(dst_dir,path)):
                    os.mkdir(join(dst_dir,path))
            blob.download_to_filename(join(dst_dir,blob.name))
            
    
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
    
def submit_pyspark_job(dataproc,projectId,clusterName,code_bucket_name,region,
                       mainFilename,otherFilesName=None,initAction=None,
                       initParams=None):
    '''
    This function submits a pyspark job to a dataproc cluster. It is also
    possible to perform some initil actions before performing the main
    job. For example, to delete all the files in a bluck on the storage.
    This can be done by passing the function performing the
    initial actions to `initAction`.
    
    Parameters
    ---------
    dataproc: dataproc client
    get this by:
        >>> dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    projectId: str
    clusterName: str
    bucket_name: str
        Name of the bucket containing job's files.
    region: str
    mainFilename: str
        the name of the main file containing the job.
    otherFilesName: str
        name of a zip file containing other files which are used in the
        main file.
    initFileName: function
        function peforming the initial actions.
    initParams: dictinary
        parameters to be passed to initAction
    '''
    
    if initAction is not None:
        print(ctime()+'...performing initialization actions...')
        initAction(**initParams)
    
    if otherFilesName is None:
        pysparkJob={'mainPythonFileUri':'gs://{}/{}'.\
                    format(code_bucket_name, mainFilename)}
    else:
        pysparkJob={'mainPythonFileUri':'gs://{}/{}'.\
                    format(code_bucket_name, mainFilename),
                    'pythonFileUris':'gs://{}/{}'.\
                    format(code_bucket_name, otherFilesName)}
        
    job_details={'reference': {'projectId': projectId},
                 'placement': {'clusterName': clusterName},
                 'pysparkJob':pysparkJob}
    
    result = dataproc.projects().regions().jobs().submit(
                projectId=projectId,
                region=region,
                body={'job':job_details}).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    return job_id
    return job_details

def delete_bucket(bucket_name):
    '''
    delete all blobs in a bucket.
    '''
    
    storage_client = storage.Client()
    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket is None:
        pass
    else:
        for b in bucket.list_blobs():
            b.delete()
        
        
    
    
    
    
    