from flask import Flask, render_template, request
from utilities import amazon_transcribe, extract_asrOutput
import json
from meeting_summarization import generate_complete_file
import asyncio
from Handler import process_single

app = Flask(__name__)

@app.route('/', methods=["POST", "GET"])
def method_name():

    return 'Hi this is the Deepcon Server'


@app.route('/getcode', methods=["POST", "GET"])
def process_input():
    

    code = request.args.get('process_code')
    print("code received: ", code)
    file_name = f"{code}.mp3"
    res = amazon_transcribe(audio_file_name= file_name,
                            max_speakers=2)
    
    url = res['TranscriptionJob']['Transcript']['TranscriptFileUri']
    print("----------------------------------------------")
    print(url)
    # save file locally 
    path_to_raw_transcript = "output//raw-transcripts"
    extract_asrOutput(url, path_to_raw_transcript, code)

    # process asrOutput 
    path_to_file = f"{path_to_raw_transcript}//{code}.json"
    process_single(path_to_file, code)

    print("response done")
    print("-----------------------------------")
    print(res)
        
    print("response saved")
    
    print(f'generating final document for {code}')
    generate_complete_file(f"output//processed-transcripts/{code}.txt")
    print(f"final document processed for {code}")

    return 'process complete'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
