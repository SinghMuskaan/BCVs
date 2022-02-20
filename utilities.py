from urllib.request import urlopen
import boto3
import pandas as pd
import time
import urllib
import json
import os
import certifi
import ssl
from dotenv import load_dotenv
load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
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

# save asrOutput file locally
def extract_asrOutput(asr_url, path_to_raw_transcript: str, code):
  
  print("hello")
  response = urllib.request.Request(url=asr_url, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
                      'AppleWebKit/537.11 (KHTML, like Gecko) '
                      'Chrome/23.0.1271.64 Safari/537.11',
                      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8',
                      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                      'Accept-Encoding': 'none',
                      'Accept-Language': 'en-US,en;q=0.8',
                      'Connection': 'keep-alive'})
  print(response)
  response = urlopen(response, timeout=10)
  print("hello2")
  data = json.loads(response.read())
  path_to_file = f"{path_to_raw_transcript}//{code}.json"
  with open(path_to_file, "w") as fp:
    json.dump(data, fp)
    
def main():
      
  url = "https://s3.ap-south-1.amazonaws.com/aws-transcribe-ap-south-1-prod/859116575611/9JOtPI0rhoJOSQN1/0b5141eb-d165-475b-ae27-b2f2b64f8280/asrOutput.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjENb%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCmFwLXNvdXRoLTEiRzBFAiBsFlnxUVH7TklcURb5vaQAQs%2BoOKsJCdFEoCdIWNoetwIhAORlhPBlrQtJGA%2BT7NFehbda%2BHdZj9eooEbueDffBJxHKvwDCC8QAxoMNjYzMDE5MTE5ODM2IgxncE8Sk%2Bvympw000Uq2QPyBaR%2BwbOQZVVP8G18rc76c4QgXQQklBwgiqECgbdsJOcN1HFJekDHFdsgkLNjc3OtQDZ8TxrRJPwznY0sxpaMVDVY2b0vbNdTF%2FVgmsqESInaij%2Bp1iMjUeZ1sxQiMIcQEuedTvi9Wu4YWKlrNImTjI5y5KeV6Gm5zk6BRrvMVv1%2BAdcBoeBqmYYYagr3z157jUdiHh5gzIBd1KQY%2BaS7oFC7NgG1MZmVVUtDhx2hQTboLl1r6D%2BiDzdppu3Y%2B3VrgmUaW2NNJKPPVq3BR8tTq2VOVR71FTVaKJbtr%2FxjG5rBiyUmN15H31XMkHSgkUGCQFi357QOXR%2F5ROavLX4NXjBSliLXqV2Ix99a%2BFemMXzngL09dybBaqwc8sskDPyk4GqNTelS8djJpb0RutfrQuCOQeTNADoZs0n%2FJqmdclml8nPOupL7Bp9mGZg5Qx%2BZPawFpMZV3AQWUNyTBlNVudgkXn8rHpMzmR%2FYMrXxkU8QIKUkeuOcFQREiIue2f%2FgoNHJlwYbdO9nw9xvty%2BuB4JvGj71KxCip0v9zEc4WVWGP8Mqe%2BitZwedtWejhf1zjHCDVy17yCX1OY%2FNPoU0dghs8iAKXlwFmjEPX4C6AdnQSkSSMaV%2F2TC3k8mQBjqlAVIzjFIw5OdkHsqKawogUT2bLdkM8%2BT6%2FZwwVSDnaetQ8ZDrw1IwU%2Boez8tM0Gya6%2FwtYOcgmT2EcC3trpzFoOxIFGO6bqyLHRl0XIsHcskYazTl0SQsf5ELLSreOkCMjwfgsDixn0DzT2sHP90LIsX1C3wldJ6OJgKI%2Fs%2BKXKTzV8HtUEorZY8NA3C7ENjf9lXI52W89PYIToVxDJ3v3apc8tve0Q%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220220T144848Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=ASIAZUXYE4TOLOOHI76K%2F20220220%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Signature=df50b00a53a7d7b3cd84fde90d4f98dfa3d9b1063d728691269b9f2eda87a32a"
  path_to_raw_transcript = ""
  extract_asrOutput(url, path_to_raw_transcript, 'example1')
if __name__ == '__main__':
  
  main()
