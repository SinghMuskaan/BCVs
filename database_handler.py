from pymongo import MongoClient
import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
load_dotenv()


ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")


def s3_upload(processed_file_path, process_code):
    
    session = boto3.Session(
    aws_access_key_id= ACCESS_KEY,
    aws_secret_access_key= SECRET_KEY ,
    )
    s3 = session.resource('s3')
    key = f'processed_{process_code}'
    s3.meta.client.upload_file(Bucket='deepcon-processed-minutes', Key=key, Filename = processed_file_path)
    print("Upload Successful")
    

def update_values(process_code: str, processing_status):
    
    file_path = f'output/meeting-minutes/{process_code}.txt'
    status = False
    try:
        conn = MongoClient()
        status = True
        print("Connected successfully!!!")
    except:
        status = False  
        print("Could not connect to MongoDB")
        
    client = MongoClient("mongodb+srv://Majorcms:Majorcms@khoj.nqwbp.mongodb.net/khoj?retryWrites=true&w=majority")
    db = client.MajorCMS
    collection = db.MajorCMS
    s3_upload(file_path, process_code=process_code)  
    myquery = { "process_code": process_code }
    newvalues = { "$set": {"processing_status": processing_status,
                           "processed_minutes_link": f's3://deepcon/processed/{process_code}.txt'} }
    res = collection.update_one(myquery, newvalues)
          

    return status