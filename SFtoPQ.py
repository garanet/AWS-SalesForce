# DEFINE MODULES 
import sys, os, boto3, pytz 
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.sql.types import * 
from pyspark.sql import SQLContext 
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.job import Job 
from datetime import datetime 
 
# DEFINE VARIABLES (change it) 
source_bucket = '' 
source_prefix = 'salesforce_output/landing/' 
target_bucket = '' 
target_prefix = 'salesforce_output/parquet/' 
 
# CREATE SPARK/S3 SESSIONS 
sc = SparkContext() 
glueContext = GlueContext(sc) 
spark = glueContext.spark_session 
logger = glueContext.get_logger() 
s3_client = boto3.client('s3') 
s3_resource = boto3.resource('s3') 
logger.info("s3_key:" + 'START') 
 
# FORMAT READ DATA PARTITION 
def data_partition(): 
    date_time = datetime.now() 
    pst = pytz.timezone('Europe/Amsterdam') 
    date_time = date_time.astimezone(pst) 
    year = date_time.strftime("%Y") 
    month = date_time.strftime("%m") 
    day = date_time.strftime("%d") 
    data_partition = '{}/{}/{}/'.format(year,month,day) 
    logger.info("s3_key:" + 'data_partition DONE') 
    return(year,month,day,data_partition) 
 
# READ PATH FOR THE FOLDERS 
def list_folders(bucket_name): 
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix, Delimiter='/') 
    logger.info("s3_key:" + 'list_folders DONE') 
    for content in response.get('CommonPrefixes', []): 
        yield content.get('Prefix') 
 
# SCAN IF THERE ARE FILES INSIDE THE PARTIONED LANDING ZONE 
def scan_folder(): 
    folder_list = list_folders(source_bucket) 
    partition = data_partition() 
    partition = partition[3] 
    for folder in folder_list: 
        Prefix = folder + partition 
        pathin = [] 
        for object_summary in s3_resource.Bucket(source_bucket).objects.filter(Prefix=Prefix): 
            if s3_client.head_object(Bucket=source_bucket, Key=object_summary.key)['ContentLength'] == 0: 
                logger.info("s3_key:" + 'No files found here') 
                continue 
            else: 
                pathin.append('s3://{}/'.format(source_bucket)+object_summary.key) 
    return(pathin,folder) 
 
# CREATE PARTITION FOR THE DEFINE ZONE 
def write_partion(): 
    now = data_partition() 
    year = 'year=' + now [0] 
    month = 'month=' + now[1] 
    day = 'day=' + now[2] 
    target_f = scan_folder() 
    folder = target_f[1].split('/') 
    folder = folder[2] 
    tosave = 's3://' + target_bucket + '/' + target_prefix + folder + '/' + year + '/' +  month + '/' + day + '/' 
    logger.info("s3_key:" + tosave) 
    return(tosave) 
 
# SAVE TO REFINE IN PARQUET     
def create_parquet(): 
    target_p = scan_folder() 
    pathin = target_p[0] 
    if len(pathin)==0: 
        logger.info("s3_key:" + 'Its empty') 
        return False 
    else: 
        read_settings = {"header": None, "inferSchema": True, "sep": '\t',"multiLine": True,"escape": '"'} 
        try: 
            tosave = write_partion() 
            logger.info("s3_key:" + 'I try the DF') 
            df = spark.read.format('csv').load( [path for path in pathin], **read_settings ) 
            df.write.parquet(tosave) 
            logger.info("s3_key:" + 'Saved') 
        except Exception as e: 
            logger.info("s3_key:" + 'SF-skipping-source-error: ') 
            logger.info("s3_key:" + str(e))  
             
# RUN THE SCRIPT 
create_parquet() 
