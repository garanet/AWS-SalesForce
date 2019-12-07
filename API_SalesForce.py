### Define Modules
import datetime, time, jwt, requests, s3fs, boto3, base64
import pandas as pd
from config import *
from io import StringIO, BytesIO
from pandas import DataFrame, Series
from botocore.exceptions import ClientError
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

### Define Accounts
bucket = ''
main_folder = 'Salesforce/Daily_Update/'
consumer_id = ''
username = ''

### READ THE PEM FILE FROM SECRET MANAGERS
def get_secret():
    secret_name = ""
    region_name = ""
    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return secret
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret
        
### Define the Login Function to create the PAYLOAD
def jwt_login(consumer_id, username, private_key):
    endpoint =  'https://login.salesforce.com'
    jwt_payload = jwt.encode(
        {
            'iss': consumer_id,
            'sub': username,
            'aud': endpoint,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
        },
        private_key,
        algorithm='RS256'
    )
    # Send the request to get the TOKEN
    result = requests.post(endpoint + '/services/oauth2/token', data={'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer','assertion': jwt_payload})
    body = result.json()
    sf = Salesforce(instance_url=body['instance_url'], session_id=body['access_token'])
    # Print LOGIN errors
    if result.status_code != 200:
        raise SalesforceAuthenticationFailed(body['error'], body['error_description'])
    return Salesforce(instance_url=body['instance_url'], session_id=body['access_token'])
    
### Define the Queries Dict
def query_full_list():
    query_list = dict({
        'query_survey_task':'',
        'query_survey_answer':'',        
        'query_user':'',
        'query_contact':'',
        'query_account':'',        
        'query_order':''
        })
    return (query_list)

### Define the Queries PERIOD 
def query_custom_list(when,N):
    if when == 'DELETED':        x = "WHERE isDeleted = true" # Append the isDeleted query
    if when == 'YESTERDAY':      x = "WHERE LastModifiedDate = YESTERDAY" # Append the CreateDate from Yesterday :- Starts 12:00:00 the day before and continues for 24 hours.
    if when == 'THIS_MONTH':     x = "WHERE LastModifiedDate < THIS_MONTH" # If you need for the last month
    if when == 'LAST_MONTH':     x = "WHERE CloseDate > LAST_MONTH" # If you need for the last month    
    if when == 'LAST_N_DAYS':    x = "WHERE LastModifiedDate = LAST_N_DAYS:" + N
    if when == 'TODAY':          x = "WHERE LastModifiedDate >= TODAY"
    if when == 'LAST_WEEK':      x = "WHERE LastModifiedDate > LAST_WEEK"
    if when == 'THIS_WEEK':      x = "WHERE LastModifiedDate < THIS_WEEK"
    # Retrive the queries and remake the array
    query_list = query_full_list()
    # Make filters if in the query already exists the WHERE sentence
    lookup = {}
    for keys, values in query_list.items():
        if "WHERE" in values:
            x = x.replace('WHERE', ' AND ')
            x = x.replace('OR', ' AND ')
        if "query_user" in keys and when == "DELETED":
            continue
        name =  "".join((keys, '_'+when))
        querycust = " ".join((values, x))
        lookup[name] = querycust
    return lookup

### Starting the query_list loop
def query_loop(query_list, q_type):
    # LOGIN
    private_key = get_secret()
    sf = jwt_login(consumer_id, username, private_key)
    # FILTER THE QTYPE LIST
    for filename,query in query_list.items():
        start = time.time()
        print()
        print(">> Running %s at %s" % (filename,now()))
    # Run the Normal query for the FULL
        filename = filename+".csv"
    # Q_TYPE MORE = query_more / ALL = SF.query_all
        if q_type == 'MORE':
            queryMORE(sf,filename,start,query)
        elif q_type == 'ALL':
            queryALL(sf,filename,start,query)
        else:
            print('Error: Select a qtype query.')
            SystemExit(17)
            exit()
    return True

### RUN THE QUERY MORE 2000 rows
def queryMORE(sf,filename,start,query):
        print('SF will provide a QUERY_MORE')
        qry_result = sf.query(query)    
        print('SF QueryResult TotalSize {0}'.format(qry_result['totalSize']))
        df = DataFrame(qry_result['records'])
        try:
            df = df.drop('attributes', axis=1)
        except:
            df = df
        # MULTIPART S3FS
        saveS3(filename,df)
        # CHECK IF THERE ARE MORE OF 2000 ROWS
        is_done = qry_result['done'];
        while not is_done:
            try:
                qry_result = sf.query_more(qry_result['nextRecordsUrl'], True)
                df = df.append(DataFrame(qry_result['records']), sort=False)
                try:
                    df = df.drop('attributes', axis=1)
                except:
                    print('MORE EXCEPT WARNING')
                    df = df
        # MULTIPART S3FS
                saveS3(filename,df)
        # CHECK IF THERE ARE MORE OF 2000 ROWS
                if qry_result['done']:
                    is_done = True;
                    print('SF completed')
                    df.iloc[0:0]
            except NameError as e:
                    print('Error: SOQL failed ' + e)
                    SystemExit(17)
        return (end_msg(df,filename,start))

### RUN THE QUERY ALL
def queryALL(sf,filename,start,query):
        print('SF will provide a QUERY_ALL')
        qry_result = sf.query_all(query)
        print('SF QueryResult TotalSize {0}'.format(qry_result['totalSize']))
        items = {
	       val: dict(qry_result["records"][val])
	       for val in range(qry_result["totalSize"])
	       }
        try:
            df = pd.DataFrame.from_dict(items, orient="index").drop(["attributes"], axis=1) # orderreddict to dataframe
        except:
            print('FULL EXCEPT WARNING')
            df = DataFrame(qry_result['records'])
        # MULTIPART S3FS
        saveS3(filename,df)
        df.iloc[0:0]
        return (end_msg(df,filename,start))

### Simple DATE/TIME function
def now():
    now = datetime.datetime.now()
    now = now.strftime("%H:%M:%S %d-%m-%Y")
    return (now)

### Make the Partition/Folder name
def folder_name():
    folder_data = datetime.datetime.now()
    year = folder_data.strftime("%Y")
    month = folder_data.strftime("%m")
    day = folder_data.strftime("%d") 
    structure = (year +'/'+ month+'/'+day+'/')
    partition = main_folder+structure
    return (partition)
    
### Save to S3
def saveS3(filename,df):
    folder=folder_name()
    ### MULTIPART S3FS
    s3 = s3fs.S3FileSystem(anon=False)
    try:
        with s3.open('s3://'+bucket+'/'+folder+filename,'w') as f:
            df.to_csv(f,sep='\t', encoding='utf-8',index=False)
    except:
        print('Error: I cannot save to S3')
        exit()
    return ('Saved to S3')

### SHOW THE TOTAL RESULTS TIME
def end_msg(df,filename,start):
        csv_buffer = StringIO()
        end = time.time()
        t = ((end - start))
        try:
            totaltime.append = (t + totaltime)
        except NameError:
            totaltime = 0
        size_buffer=sys.getsizeof(csv_buffer)
        gz_buffer = BytesIO()
        ### CLEAN THE MEMORY
        df.iloc[0:0]
        csv_buffer.flush()
        return print("Total time for %s is %s at %s" % (filename, (end - start), now()))

### Run the functions ###
def runit(when, Qtype, N): 
    if when == 'FULL': # FULL QUERY, WILL TAKES TIME.
        query_list = query_full_list() 
        varsloop = query_loop(query_list, Qtype)
    else: # CUSTOM QUERY LIKE YESTERDAY, LAST_MONTH
        query_list = query_custom_list(when, N)
        varsloop = query_loop(query_list, Qtype) 

### SELECT YOUR PERIOD AND RUN THE SCRIPT ###
### SPECIFY THE N values as DAYS for the LAST_N_DAYS ###
### MORE = SF.query_more # ALL = SF.query_all ###
#runit('FULL', 'ALL', '')
runit('YESTERDAY', 'ALL', '')
#runit('THIS_MONTH', 'MORE', '')
#runit('LAST_MONTH', 'MORE', '')
#runit('DELETED', 'MORE', '')
#runit('LAST_N_DAYS', 'MORE' ,'3')
#runit('THIS_WEEK', 'MORE', '')
#runit('LAST_WEEK', 'False', '')
#runit('TODAY', 'MORE', '')
