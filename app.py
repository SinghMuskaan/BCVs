from flask import Flask, render_template, request
from utilities import amazon_transcribe
import json
import asyncio

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
    
    print("response done")
    print("-----------------------------------")
    print(res)
        
    print("response saved")

    return 'process complete'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
