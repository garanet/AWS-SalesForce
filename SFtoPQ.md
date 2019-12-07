# Sales Force GLUE/Athena

This README file explains how the script works.

  - Information
  - Requirements
  - Configuration
  - Functions
  - Logic
 
### Information
The SFtoPQ.py script reads files from the landing zone and will save to the refine zone in parquet format.
It is flexible and modular, easy to add new functionality or modify the current.

From a partitioned landing zone [LANDING-ZONE/YEAR/MM/DD] -> 
SCAN FOR THE LAST FILES CREATED -> 
MAKE THE LIST OF FOLDERS WITH CSV FILES -> 
SAVE TO REFINE ZONE IN PARQUET [REFINE-ZONE/year=YEAR/month=MM/day=DD]

### Requirements
  - Run with Glue or with a PythonSpark enviroment.
  - A S3 bucket to save files from SalesForce.
  - The rights permissions to get access in SF or save files to S3.

### Configuration
Once you opened the cvstopq.py script, you have to change the below section.
```sh
# DEFINE VARIABLES (change it)
source_bucket = 'NAMEBUCKET'
source_prefix = 'salesforce_output/landing/'
target_bucket = 'NAMEBUCKET'
target_prefix = 'salesforce_output/parquet/'
```
### Functions
The section explains how the script's functions are working.

* The `def data_partition():` The function creates the partition based of the current day to permit to boto3 to read from the right path. We supposed the previous script (lambda) saves to S3 like: S3://BUCKET/LANDING_ZONE/NAME_QUERY/2019/08/02/name_query.csv .
* The `def list_folders(bucket_name)` S3 Connection to get the folders and returns the right path.
* The `def scan_folder()` From the previous path, retrieves filename.
* The `def create_parquet()` It saves to the S3 bucket the files in Parquet format.
* The `def write_partion()` Makes the new partition for the parquet format as [year=YEAR/month=MM/day=DD] .

### Logic
If you want to run the script you have to call the create_parquet() function like:
create_parquet()
The functions are working like:
{create_parquet} ---> {scan_folder} ---> {list_folders} ---> {data_partition} ---> {write_partion} ---> {create_parquet}.
