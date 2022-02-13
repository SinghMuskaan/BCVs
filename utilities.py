import boto3
import pandas as pd
import time
import os
from dotenv import load_dotenv
load_dotenv()

ACCESS_KEY = os.getenv("aws_access_key_id")
SECRET_KEY = os.getenv("aws_secret_access_key")
REGION_NAME = os.getenv("region_name")

transcribe = boto3.client('transcribe',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          region_name=REGION_NAME
)

def check_job_name(job_name):
    job_verification = True
  
    existed_jobs = transcribe.list_transcription_jobs()
  
    for job in existed_jobs['TranscriptionJobSummaries']:
        if job_name == job['TranscriptionJobName']:
            job_verification = False
            break

    if job_verification == False:
        command = input(job_name + " has existed. \nDo you want to override the existed job (Y/N): ")
        if command.lower() == "y" or command.lower() == "yes":
            transcribe.delete_transcription_job(TranscriptionJobName=job_name)
        elif command.lower() == "n" or command.lower() == "no":
            job_name = input("Insert new job name? ")
            check_job_name(job_name)
        else: 
            print("Input can only be (Y/N)")
            command = input(job_name + " has existed. \nDo you want to override the existed job (Y/N): ")
    return job_name

def amazon_transcribe(audio_file_name, max_speakers = -1):
    
  if max_speakers > 10:
    raise ValueError("Maximum detected speakers is 10.")

  job_uri = "https://deepcon.s3.ap-south-1.amazonaws.com/" + audio_file_name  
  job_name = (audio_file_name.split('.')[0]).replace(" ", "")
  
  # check if name is taken or not
  job_name = check_job_name(job_name)
  
  if max_speakers != -1:
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat=audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings = {'ShowSpeakerLabels': True,
                  'MaxSpeakerLabels': max_speakers
                  }
    )
  else: 
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat=audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings = {'ShowSpeakerLabels': True
                  }
    )    
  
  while True:
    result = transcribe.get_transcription_job(TranscriptionJobName=job_name)
    if result['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
        break
    time.sleep(15)
  if result['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
    data = pd.read_json(result['TranscriptionJob']['Transcript']['TranscriptFileUri'])
  return result

