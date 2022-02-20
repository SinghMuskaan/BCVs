import json
import datetime
import codecs
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

tqdm.pandas()

# utility
def process_data_dict(data_dict: dict):
  data_df = pd.DataFrame(data_dict)
  data_df.drop(index=[0], inplace=True)
  return data_df

def get_json(path_to_file):
  with open(path_to_file, 'r') as openfile:
      articles = json.load(openfile)
  return articles


# generate structured information from aws transcribe transcripts
def process_aws_transcribe_output(filename: str):
  data_dict = defaultdict(list)
  print ("Filename: ", filename)
  with codecs.open(filename+'.txt', 'w', 'utf-8') as w:
    with codecs.open(filename, 'r', 'utf-8') as f:
      data=json.loads(f.read())
      labels = data['results']['speaker_labels']['segments']
      speaker_start_times={}
      for label in labels:
        for item in label['items']:
          speaker_start_times[item['start_time']] =item['speaker_label']
      items = data['results']['items']
      lines=[]
      line=''
      time=0
      speaker='null'
      i=0
      for item in items:
        i=i+1
        content = item['alternatives'][0]['content']
        if item.get('start_time'):
          current_speaker=speaker_start_times[item['start_time']]
        elif item['type'] == 'punctuation':
          line = line+content
        if current_speaker != speaker:
          if speaker:
            lines.append({'speaker':speaker, 'line':line, 'time':time})
          line=content
          speaker=current_speaker
          time=item['start_time']
        elif item['type'] != 'punctuation':
          line = line + ' ' + content
      lines.append({'speaker':speaker, 'line':line,'time':time})
      for dicts in lines:
        data_dict['speaker'].append(dicts['speaker'])
        data_dict['line'].append(dicts['line'])
        data_dict['time'].append(dicts['time'])
      # save to txt
      sorted_lines = sorted(lines,key=lambda k: float(k['time']))
      for line_data in sorted_lines:
        line='[' + str(datetime.timedelta(seconds=int(round(float(line_data['time']))))) + '] ' + line_data.get('speaker') + ': ' + line_data.get('line')
        w.write(line + '\n\n')
  return process_data_dict(data_dict)

# generate final normalized multi-speaker transcripts 
def generate_normalized_output(data_df: pd.DataFrame):
  unique_speakers = list(data_df['speaker'].unique())
  person_text = "PERSON"
  speaker_mappings = dict()
  count = 0

  for speaker in unique_speakers:
    speaker_mappings[speaker] = f"{person_text}{count}"
    count += 1

  for key in speaker_mappings.keys():
    data_df['speaker'].replace(to_replace=[key], value = speaker_mappings[key], inplace=True)

  sentences = data_df.progress_apply(
      lambda row: f"({row['speaker']}) {row['line']}",
      axis=1
  )
  return sentences

def process_single(filename: str, code):
    data_df = process_aws_transcribe_output(filename)
    sentences = generate_normalized_output(data_df)

    output_dir = "output//processed-transcripts"
    save_location = output_dir + '//' + code + '.txt'
    with open(save_location, 'w') as filehandle:
        for sentence in sentences:
            filehandle.write('%s\n\n' % sentence)


if __name__ == "__main__":
    process_single(filename="output//raw-transcripts//asrOutput.json", code="asefasawdac")
