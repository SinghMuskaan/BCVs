from flask import Flask, render_template, request
from utilities import amazon_transcribe
import json


app = Flask(__name__)



@app.route('/', methods=["POST", "GET"])
def method_name():

    return 'Hi this is the Deepcon Server'


@app.route('/getcode', methods=["POST", "GET"])
def lawda():
    code = request.args.get('process_code')
    print("code received: ", code)
    file_name = f"{code}.mp3"
    res = amazon_transcribe(audio_file_name= file_name,
                            max_speakers=2)
    
    print("response done")
    with open('output.json', 'w') as outfile:
        json.dump(res, outfile)
        
    print("response saved")

    return 0


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)