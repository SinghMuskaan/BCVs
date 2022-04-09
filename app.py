from flask import Flask, render_template, request
from utilities import amazon_transcribe, extract_asrOutput
import json
from meeting_summarization import generate_complete_file
from mailing_module import send_email

from database_handler import update_values, find_value
from database_handler import update_values
from keyphrase_extraction import generate_keywords
from isometric_translation import generate_translated_document

# import asyncio
from Handler import process_single

app = Flask(__name__)

@app.route('/', methods=["POST", "GET"])
def method_name():

    return 'Hi this is the Deepcon Server'


@app.route('/getcode', methods=["POST", "GET"])
def process_input():
    # comment
    code = request.args.get('process_code')
    receiver_email = request.args.get('receiver_email')
    receiver_name = request.args.get('receiver_name')
    sender = "deepconteam@gmail.com"
    print("code received: ", code)
    print("Receiver email: ", receiver_email)
    print("Receiver name: ", receiver_name)
    file_name = f"{code}.mp3"
    res = amazon_transcribe(audio_file_name=file_name,
                            max_speakers=6)
    
    url = res['TranscriptionJob']['Transcript']['TranscriptFileUri']
    print("----------------------------------------------")
    # save file locally 
    path_to_raw_transcript = "output//raw-transcripts"
    extract_asrOutput(url, path_to_raw_transcript, code)

    # process asrOutput 
    path_to_file = f"{path_to_raw_transcript}//{code}.json"
    process_single(path_to_file, code)

    print("response done")
    print("----------------------------------------------") 
    print("response saved")

    # --new-- #
    generate_keywords(process_code=code, path_to_transcripts_directory="output/processed-transcripts", path_to_keyword_directory="output/processed-keywords", ngram=3)
    print(f'generated keywords for {code}')
    # ------- #

    generate_complete_file(f"output//processed-transcripts/{code}.txt", code)
    print(f"final document processed for {code}")

    generate_translated_document(process_code=code)
    print(f'generated french minutes for {code}')

    update_values(process_code=code,
                  processing_status=True,
                  translated_status=True)

    transcript_link, minutes_link, translated_link = find_value(code)

    email_res = send_email(
        process_code=code,
        receivers_name=receiver_name,
        receiver_email=receiver_email,
        sender=sender,
        transcript_link=transcript_link,
        minutes_link=minutes_link,
        translated_link=translated_link
    )

    return 'process complete'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
