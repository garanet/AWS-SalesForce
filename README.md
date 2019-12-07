# AWS-SalesForce
Lambda Python 3 script to for AWS for the Sales Forces Query.
# Sales Force API
 
This README file explains how the script works. 
The API_SalesForce.py script contains the API with queries and functions.
The aws_sf_lambda.py script contains the API for a AWS Lambda. 
The reqs.txt file is needed for the installation. 
The XLS file is a Report measurement. 
 
  - Information 
  - Requirements 
  - Configuration 
  - Functions 
  - Logic 
  
### Information 
This script permits to connect and run queries to the SOQL (Salesforce Object Query Language). It uses the JWT (Java Web Token) and the Simple SalesForce python's libraries for the Authentication. 
It is flexible and modular, easy to add new functionality or modify the current. 
This version runs queries and saves as a DataFrame the results to an AWS S3 Bucket in CSV format. 
 
| Knowlodge | URL | 
| ------ | ------ | 
| SOQL | https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_sosl_intro.htm | 
| SimpleSalesForce | https://pypi.org/project/simple-salesforce/ | 
| JWT with Python | https://dev.to/apcelent/json-web-token-tutorial-with-example-in-python-23kb | 
| DATEFORMAT SOQL | https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_select_dateformats.htm | 
 
### Requirements 
  - Python3 
  - Libraries and modules: datetime, time, json, jwt, pandas, requests, config, io, s3fs, simple_salesforce. 
  - The Private Key (PEM) and the Customer Key from your SF Admin. 
  - Info how the SOQL and the JWT are working. 
 
You need also: 
  - A S3 bucket to save files from SalesForce. 
  - A EMR or EC2 with enough RAM (for the big dump). 
  - The rights permissions to get access in SF or save files to S3. 
  - The SalesForce Objects Schema from your SF Admin. 
 
### Configuration 
Before running the script you may install the dependencies and change config in the script. 
```sh 
$ pip3 install -r reqs.txt 
``` 
Once you opened the script, you have to change the below section. 
```sh 
### Define Accounts  
bucket = 'INSERT_BUCKET_NAME'  
folder = 'salesforce_output/'  
consumer_id = 'INSERT_THE_CONSUMER_KEY'
username = 'INSERT_THE_USERNAME'  
``` 

```sh 
### READ THE PEM FILE FROM SECRET MANAGERS
def get_secret():
    secret_name = ""
    region_name = ""
``` 

If you have a query do add or you need to modify one, you need to change this section. 
```sh 
### Define the Queries Dict
def query_full_list():
    query_list = dict({ 
``` 
 
If you want to skip the SF Query_More, you have to modify this section in runit() 
```sh 
`runit('WHEN', 'QTYPE', '')` (Query object in ALL/Query_All or if it is too big use MORE/Query_More) 
``` 
 
### Functions 
The section explains how the script's functions are working. 

* The `def get_secret()` It calls the PEM key from SECRET MANAGER 
* The `def jwt_login()` It's the real API Call to get the SESSIONID and Authenticate in SF.
* The `def query_full_list()` There is the LIST (Dict) of the queries to run. You can modify or add and remove them as your preferences. Remember, they are in SOQL format. 
* The `def query_custom_list(when,N)` There are different options in SOQL and SF permits to retrieve objects in a different period like  YESTERDAY, THIS_MONTH, LAST_N_DAYS, or DELETE. 
* The `def query_loop(query_list, q_type)` It's the query loop, it define the type of query to run.
* The `def queryMORE(sf,filename,start,query)` It's the query_more, it gets 2000 rows for each call as sf.query + nextRecordsUrl + sf.query_more method. The function also filters and creates a dataframe with panda. 
* The `def queryALL(sf,filename,start,query)` It's the query_all, it gets all rows in one call. The function also filters and creates a dataframe with panda. 
* The `def now()` Simple DATE/TIME function
* The `def folder_name()` Makes the partition folders based of current data.(YEAR/MONTH/DATE)
* The `def saveS3(filename,df)` Save to the S3 bucket.
* The `def end_msg(df, filename, start)` It shows the Total query time. 
* The `def runit(when,"ALL/MORE",'N'):` It's the last function that calls the in seq the previous functions. Specify if you want a Query_more or a Query_All. Specify the N value if your run the LAST_N_DAYS query.
 
 
### Logic 
If you want to run the script you have to call the runit() function: 
- Check the WHEN period var in runit() function and the Qtype var if you want run the QUERY_ALL (ALL) or the QUERY_more (MORE).
- Based of what queries you need, uncomment the function runit('') to run. 
 
 
- `runit('FULL', 'ALL' ,'')` For a full Dump and MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('YESTERDAY', 'ALL','')` Starts 00:00:00 the day before and continues for 24 hours. ORE = SF.query_more, ALL = SF.query_all 
  
- `runit('TODAY', 'ALL','')` Starts 00:00:00 of the current day and continues for 24 hours. MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('THIS_MONTH', 'ALL','')` Starts 00:00:00 on the first day of the month that the current day is in and continues for all the days of that month. MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('LAST_MONTH', 'ALL','')` Starts 00:00:00 on the first day of the month before the current day and continues for all the days of that month. MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('DELETED', 'ALL','')` Objects deleted (but not available for all queries) MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('LAST_N_DAYS', 'ALL','N')` Objects from the last N days, You may change the N value. MORE = SF.query_more, ALL = SF.query_all 
  
- `runit('LAST_WEEK', 'ALL','')` Starts 00:00:00 on the first day of the week before the most recent first day of the week and continues for seven full days. Your locale determines the first day of the week. MORE = SF.query_more, ALL = SF.query_all 
 
- `runit('THIS_WEEK', 'ALL','')`   Starts 00:00:00 on the most recent first day of the week before the current day and continues for seven full days. Your locale determines the first day of the week. MORE = SF.query_more, ALL = SF.query_all 
  
 
{SF-LOGIN} --
     |-> {SECRET} --
          |-> {SESSIONID} --
              |-> {QUERY_more 2000 ROWS IN A CYCLE} / {QUERY_all a full dump without a cycle} --
                                                                                                                                                                                                                                                                                                                                                                                           
* The `def get_secret()` It calls the PEM key from SECRET MANAGER 
* The `def jwt_login()` It's the real API Call to get the SESSIONID and Authenticate in SF.
* The `def query_full_list()` There is the LIST (Dict) of the queries to run. You can modify or add and remove them as your preferences. Remember, they are in SOQL format. 
* The `def query_custom_list(when,N)` There are different options in SOQL and SF permits to retrieve objects in a different period like  YESTERDAY, THIS_MONTH, LAST_N_DAYS, or DELETE. 
* The `def query_loop(query_list, q_type)` It's the query loop, it define the type of query to run.
* The `def queryMORE(sf,filename,start,query)` It's the query_more, it gets 2000 rows for each call as sf.query + nextRecordsUrl + sf.query_more method. The function also filters and creates a dataframe with panda. 
* The `def queryALL(sf,filename,start,query)` It's the query_all, it gets all rows in one call. The function also filters and creates a dataframe with panda. 
* The `def now()` Simple DATE/TIME function
* The `def saveS3(filename,df)` Save to the S3 bucket.
* The `def end_msg(df, filename, start)` It shows the Total query time. 
* The `def runit(when,"ALL/MORE",'N'):` It's the last function that calls the in seq the previous functions. Specify if you want a Query_more or a Query_All. Specify the N value if your run the LAST_N_DAYS query.
