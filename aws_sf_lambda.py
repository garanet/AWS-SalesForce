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
bucket = 'INSERT_BUCKET_NAME'
folder = 'salesforce_output/'
consumer_id = 'INSERT_THE_CONSUMER_KEY'
username = 'INSERT_THE_USERNAME'

### USE PRIVATE KEY INSTEAD OF SECRET MANAGER
#private_key = open('INSERT_THE_PRIVATE_KEY_PATH', 'rt').read()

### READ THE PEM FILE FROM SECRET MANAGER
def get_secret():
    secret_name = "INSERT_SECRET_NAME"
    region_name = "INSERT_REGION"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return secret
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret
        
### Define the Login Function to create the PAYLOAD
def jwt_login(consumer_id, username, private_key, sandbox=False):
    endpoint = 'https://test.salesforce.com' if sandbox is True else 'https://login.salesforce.com'
    jwt_payload = jwt.encode(
        {
            'iss': consumer_id,
            'sub': username,
            'aud': endpoint,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
        },
        private_key,
        algorithm='RS256'
    )
### Send the request to get the TOKEN
    result = requests.post(endpoint + '/services/oauth2/token', data={'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer','assertion': jwt_payload})
    body = result.json()
    session_id = body['access_token']
    instance_url = body['instance_url']
    sf = Salesforce(instance_url=instance_url, session_id=session_id)
### Print LOGIN errors
    if result.status_code != 200:
        raise SalesforceAuthenticationFailed(body['error'], body['error_description'])
    return Salesforce(instance_url=body['instance_url'], session_id=body['access_token'])

### Define the Queries for a Full Objects DUMP.
def query_full_list():
    query_account = ""
    query_user = ""
    query_contact = ""
    
### Define the query's Array and the filename
    query_list = [
        ['query_contact', query_contact],
        ['query_user', query_user],
        ['query_account', query_account] 
    ]
    return (query_list)

### Define the Queries for the lastest updates
def query_custom_list(when):
    if when == 'DELETED':
        x = "WHERE isDeleted = true" # Append the isDeleted query
    if when == 'YESTERDAY':
        #x = "WHERE LastModifiedDate = YESTERDAY OR CreatedDate = YESTERDAY" # Append the LastModifiedDate and CreateDate from Yesterday :- Starts 12:00:00 the day before and continues for 24 hours.
        x = "WHERE CreatedDate = YESTERDAY" # Append the CreateDate from Yesterday :- Starts 12:00:00 the day before and continues for 24 hours.
    if when == 'THIS_MONTH':
        #x = "WHERE LastModifiedDate < THIS_MONTH OR CreatedDate < THIS_MONTH" # If you need LastModifiedDate/CreatedDate for the last month
        x = "WHERE CreatedDate < THIS_MONTH" # If you need for the last month
    if when == 'LAST_MONTH':
        x = "WHERE CloseDate > LAST_MONTH" # If you need for the last month    
    if when == 'LAST_N_DAYS':
        n = '2' # If you need updates since N last days
        #x = "WHERE LastModifiedDate = LAST_N_DAYS:" + n + " OR CreatedDate = LAST_N_DAYS:" + n
        x = "WHERE CreatedDate = LAST_N_DAYS:" + n
    if when == 'TODAY':    
        x = "WHERE CreatedDate >= TODAY"
    if when == 'LAST_WEEK':    
        x = "WHERE CreatedDate > LAST_WEEK"
    if when == 'THIS_WEEK':    
        x = "WHERE CreatedDate < THIS_WEEK"
        
### Retrive the queries and remake the array
    query_list = query_full_list()
    q_list = []
    for name, query in query_list:
### Make filters if in the query already exists the WHERE sentence
        if "WHERE" in query:
            x = x.replace('WHERE', ' AND ')
            x = x.replace('OR', ' AND ')
        if "query_user" in name and when == "DELETED":
            continue
        name =  "".join((name, '_'+when))
        querycust = " ".join((query, x))
        q_list += [[name, querycust]]
    return (q_list)

### Simple DATE/TIME function
def now():
    now = datetime.datetime.now()
    now = now.strftime("%H:%M:%S %d-%m-%Y")
    return (now)
    
### Starting the query_list loop
def query_loop(query_list, q_type):
    private_key = get_secret()
    sf = jwt_login(consumer_id, username, private_key, sandbox=False)
    for sub_list in query_list:
        start = time.time()
        print()
        print(">> Running %s at %s" % (sub_list[0],now()))
    ### Run the Normal query for the FULL
        filename = sub_list[0]+".csv"
        query = sub_list[1]
    ### Q_TYPE TRUE = query_more / FALSE = SF.query_all
        if q_type == 'True':
            print('SF will provide a QUERY_MORE')
            qry_result = sf.query(query)    
            print('SF QueryResult TotalSize {0}'.format(qry_result['totalSize']))
            #json_normalize(qry_result)
            df = DataFrame(qry_result['records'])
            try:
                df = df.drop('attributes', axis=1)
            except:
                df = df
        ### MULTIPART S3FS
            s3 = s3fs.S3FileSystem(anon=False)
            with s3.open('s3://'+bucket+'/'+folder+filename,'w') as f:
                df.to_csv(f,sep='\t', encoding='utf-8',index=False)            
        ### CHECK IF THERE ARE MORE OF 2000 ROWS
            is_done = qry_result['done'];
            while not is_done:
                try:
                    qry_result = sf.query_more(qry_result['nextRecordsUrl'], True)
                    df = df.append(DataFrame(qry_result['records']), sort=False)
                    try:
                        df = df.drop('attributes', axis=1)
                        
                    except:
                        print('MORE EXCEPT')
                        df = df
            ### MULTIPART S3FS
                    s3 = s3fs.S3FileSystem(anon=False)
                    with s3.open('s3://'+bucket+'/'+folder+filename,'w') as f:
                        df.to_csv(f,sep='\t', encoding='utf-8',index=False)
            ### CHECK IF THERE ARE MORE OF 2000 ROWS
                    if qry_result['done']:
                        is_done = True;
                        print('SF completed')
                        df.iloc[0:0]
                except NameError as e:
                        print('SOQL failed ' + e)
                        SystemExit(17)
            end_msg(df,filename,start)
        else:
    ### RUN THE QUERY ALL for the Customs queries
            print('SF will provide a QUERY_ALL')
            qry_result = sf.query_all(query)
            print('SF QueryResult TotalSize {0}'.format(qry_result['totalSize']))
            #json_normalize(qry_result)
            items = {
                val: dict(qry_result["records"][val])
                for val in range(qry_result["totalSize"])
                }
            try:
                df = pd.DataFrame.from_dict(items, orient="index").drop(["attributes"], axis=1) # orderreddict to dataframe
            except:
                print('FULL EXCEPT')
                #json_normalize(qry_result)
                df = DataFrame(qry_result['records'])
            ### MULTIPART S3FS
            s3 = s3fs.S3FileSystem(anon=False)
            with s3.open('s3://'+bucket+'/'+folder+filename,'w') as f:
                df.to_csv(f,sep='\t', encoding='utf-8',index=False)
            df.iloc[0:0]
            end_msg(df,filename,start)
    return ('Saved')
        
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
        df.iloc[0:0]
        csv_buffer.flush()
        return print("Total time for %s is %s at %s" % (filename, (end - start), now()))

### Run the functions ###
def runit(period, Qtype): 
    if period == 'FULL': # FULL QUERY, WILL TAKES TIME.
        query_list = query_full_list() 
        varsloop = query_loop(query_list, Qtype)
    else: # CUSTOM QUERY LIKE YESTERDAY, LAST_MONTH
        query_list = query_custom_list(period) 
        varsloop = query_loop(query_list, Qtype ) 

### SELECT YOUR PERIOD AND RUN THE SCRIPT ###
# True = SF.query_more # False = SF.query_all
def lambda_handler(event, context):
# TODO implement
    #runit('FULL', 'False')
    runit('YESTERDAY', 'False')
    #runit('THIS_MONTH', 'False')
    #runit('LAST_MONTH', 'False')
    #runit('DELETED', 'False')
    #runit('LAST_N_DAYS', 'False')
    #runit('THIS_WEEK', 'False')
    #runit('LAST_WEEK', 'False')
    #runit('TODAY', 'False')
