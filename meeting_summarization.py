from ast import keyword
import re
import json
from tqdm import tqdm
import os
import datetime 
from transformers import pipeline 

import sys
import datetime
import codecs
import pandas as pd
import textwrap
from collections import defaultdict

tqdm.pandas()

tqdm.pandas()

print('downloading model')
summarizer = pipeline("summarization", model="lidiya/bart-large-xsum-samsum")
print('model downloaded')


# list of regular expression to be forged in transcripts
reg_ex = {
    r"( *)\<(.*?)\>": '',
    r"\n": '',
    r"(  +)": ' ',
    r"\-": '',
    r"(,+)": ',',
    r"( *)(-+)": '',
    r"\[": '',
    r"\]": '',   
}

# utility methods 

# open transcript using given file_path
def open_transcript(file_path):
  document = open(file_path, "r").readlines()
  return document

# remove punctuations and special tokens
def rem_ntok(reg_ex, text):
  for key, val in reg_ex.items():
    text = re.sub(key, val, text)
  return text

# remove extra space from the text
# remove \n and concat utterance which do not start with (PERSON.*?)
def add_colon(sentence):
  eidx = re.search(r'\(PERSON(.*?)\)', sentence).end()
  if sentence[eidx] == ':':
    return sentence
  else:
    return sentence[:eidx] + ':' + sentence[eidx:]

# process roles list and remove "( and )"
def process_roles(role):
  regex = {
      r"\(": '',
      r"\)": ''
  }
  for key, value in regex.items():
    role = re.sub(key, '', role)
  return role

# remove special tokens from the processed list of roles and utterances
def remove_special_tokens(utterance):
  regex = [r'^\.\',', r'^\.\'', r'^\',', r'^,', r'^\'', r'^\.', r'^, ,', r'^\?']
  for exp in regex:
    utterance = re.sub(exp, '', utterance)
  return utterance

# retur max_lenght of list of sentences
def max_length(text_list):
  length = [len(text.split(' ')) for text in text_list]
  return max(length)

# remove short utterances precisely "4"
def preprocess_utterance(sequence):
   return_seq = [sentences for sentences in sequence if len(sentences) > 4]
   return return_seq

# insert extra roles based on generated sentences
def insert_to_roles(roles, len, idx, role, con_index):
  idx = idx + con_index
  for i in range(len):
    roles.insert(idx, role)
  return roles

# text insertion in utterance list at a particular position.
def insert_text(utterances, sequences, idx, con_index):
  idx = idx + con_index
  for text in sequences[::-1]:
    utterances.insert(idx, text)
  return utterances

# check if folder contains transcripts
def check_for_transcript(file_list):
  for file_ in file_list:
    result = re.findall("transcript", file_)
    if len(result) == 1:
      return file_
    else:
      pass
  return ValueError("File not found!")

# convert to json files and save
def to_JSON_batch(processed_dict, file_path):
  with open(file_path, "w") as file_handle:
    json.dump(processed_dict, file_handle)

def to_JSON_single(processed_dict, file_name, file_path):
  out_dict = {file_name: processed_dict}
  with open(file_path, "w") as file_handle:
    json.dump(out_dict, file_handle)

# remove newline character
def preprocess_transcripts(document):
  transcript = []
  for line in document:
    if line == "\n":
      continue
    transcript.append(line.replace("\n", "") + " ")
  return transcript

# iterate over transcript and segmentation
def parse_transcript(reg_ex, transcript):
  # updated list of transcript's text
  updateList = []
  for text in transcript:
    updateList.append(rem_ntok(
        reg_ex = reg_ex,
        text = text
    ))
  # create list of utterances
  utteranceList = []
  person_regex = [r'\(PERSON(.*)\)']
  for text in updateList:
    result = re.findall(person_regex[0], text)
    if len(result) == 1:
      utteranceList.append(add_colon(text))
    else:
      try:
        prev_text = utteranceList[-1]
        utteranceList[-1] = prev_text + text.strip() + " "
      except Exception as e:
        pass
  return utteranceList

# bifurcate transcripts into roles and utterances. 
def split_transcripts(processed_transcript):
  roles, utterances, temp_roles = [], [], []
  for text in processed_transcript:
    temp = text.split(':')
    tune = remove_special_tokens(temp[1].strip()).strip()
    tune = remove_special_tokens(tune.strip()).strip()
    tune = remove_special_tokens(tune.strip()).strip()
    if tune != '' and len(tune) > 2:
      utterances.append(tune)
      temp_roles.append(temp[0])
  for role in temp_roles:
    roles.append(process_roles(role))
  return roles, utterances

# shortning and splitting utterance sentence and assign roles!

def post_process(roles, utterances):
  mappings = {
      "idx": [],
      "utterances": [],
      "roles": []
  }
  for idx, utterance in enumerate(utterances):
    word_list = [sentence.strip() for sentence in utterance.split(' ')]
    sentence_list = [sentence.strip() for sentence in utterance.split('.')]
    # check if length of word list is greater than 150
    if len(word_list) > 150:
      sequence = []
      temp = ""
      for sentence in sentence_list:
        temp = f'{temp} {sentence}.'
        # if word limit exceeded than create a new sentence
        if len(temp.split(' ')) > 150:
          sequence.append(temp.strip())
          temp = ''
      sequence.append(temp.strip())
      # delete the sentence present in original list
      del utterances[idx]
      # preprocess and striping and removing small sentence less than 3
      sequence = preprocess_utterance(sequence)
      len_roles = len(sequence)
      # retrieve corresponding role from the roles list
      role = roles[idx]
      # delete the role present in original list
      del roles[idx]
      # mapping index, roles and utterances to mapping dictionary
      mappings["idx"].append(idx)
      mappings["utterances"].append(sequence)
      mappings["roles"].append(role)
  # Applying modifications
  con_index = 0
  for idx, index in enumerate(mappings['idx']):
    sequence = mappings['utterances'][idx]
    len_utterances = len(sequence)
    utterances = insert_text(utterances, sequence, index, con_index)
    roles = insert_to_roles(roles, len_utterances, index, mappings['roles'][idx], con_index)
    # Reflecting to the position of insertion
    # print(f'Inserted @ {index + con_index}')
    con_index = con_index + len_utterances  

  # New length after insertion
  # print(f'Length of lists after insertion {len(roles), len(utterances)}')

  # Applying changes to main dictionary
  return roles, utterances

# process single file
def process_single(path_to_file):
  document = open_transcript(path_to_file)
  transcripts = preprocess_transcripts(document)
  transcripts = parse_transcript(reg_ex, transcripts)
  roles, utterances = split_transcripts(transcripts)
  roles, utterances = post_process(roles, utterances)
  trans_dict = {
      "roles": roles,
      "utterances": utterances
  }
  return trans_dict

# pre-segmentation utility methods
# generate samsum dataset favourable sentences 
def generate_dialogues(roles, utterances):
  sentences = zip(roles, utterances)
  dialogues = [f'{role}: {utterance}' for role, utterance in sentences]
  return dialogues

# partition document for better processing.
def doc_partitioning(document, max_characters=500):
  processed_dict = dict()
  processed_dict['part_0'] = ''
  identity_generator = 'part_'
  temp = ''
  count = 0
  for sentence in document:
    key = f'{identity_generator}{count}'
    temp = temp + sentence
    if len(temp) > max_characters:
      temp = ''
      count = count + 1
      key = f'{identity_generator}{count}'
      processed_dict[key] = ''
    processed_dict[key] = processed_dict[key] +'\n'+  sentence
  return processed_dict

# summarization and minute generation.
def apply_summarizer(processed_dict):
  output = []
  for key in tqdm(processed_dict.keys(), total=len(processed_dict.keys())):
    result = summarizer(processed_dict[key])
    output.append(result[0]['summary_text'])
  return output

# split summarized paragraph into separate sentences.
def split_Sentences(summarizer_output):
  result = []
  for text in summarizer_output:
    sentences = text.split('.')
    for sentence in sentences:
      sentence = sentence.strip()
      if sentence != '':
        result.append(sentence)
  return result

# get current date.
def return_date():
  return f'Date: {datetime.datetime.now().strftime("%Y-%m-%d")}'

# prepare main-body of the meeting minutes.
def main_body(output: list):
  body = []
  body_header = 'SUMMARY- \n'
  for sentence in output:
    if len(sentence.split(' ')) > 3:
      body.append(f'-{sentence}')
  body_content = '\n'.join(body)  
  # body_Block = f'{body_header}{body_content}'
  body_Block = body_header + body_content
  return body_Block

# retrieve meeting participants
def generate_person_list(output):
  reg_ex = [r'PERSON[0-9]{1,2}']
  person_list = []
  for sentence in output:
    result = re.search(reg_ex[0], sentence)
    try:
      person_list.append(result.group())
    except:
      pass
  return list(set(person_list))

# convert generated document to text file
def convert_str_2_txt(document: str, process_code: str):
  file_name = f'output//meeting-minutes//{process_code}.txt'
  with open(file_name, "w") as text_file:
    text_file.write(document)
  return file_name

# generate attendee string
def generate_attendees(person_list):
  attendee_header = 'ATTENDEES: '
  attendee_content = attendee_header + ', '.join(person_list)
  return attendee_content

# generate meeting minute
def prepare_document(attendee_str, body, keywords, annotator = 'DeepCON'):
  Date_ = return_date()
  Document = f'{Date_}\n{attendee_str}\n{keywords}\n\n\n{body}\n\nMinuted by: {annotator}'
  return Document

# generate keyword list
def process_generated_keywords(process_code: str):
  path_to_directory = "output/processed-keywords"
  path_to_directory = os.path.normpath(path_to_directory)
  filename = f"{process_code}.txt"
  path_to_file = os.path.join(path_to_directory, filename)
  keywords = pd.read_csv(path_to_file)['text'].to_list()
  return keywords

def generate_keywords(keyword_list: list):
  keyword_header = "KEYWORDS: "
  keyword_content = keyword_header + "; ".join(keyword_list)
  return keyword_content

def generate_complete_file(path_to_file: str, process_code: str):
    # sample processing for single transcript
    # path_to_file = "output//processed-transcripts//asefasawdac.txt"
    trans_dict = process_single(path_to_file)
    trans_dict.keys()

    # merge roles and utterances in conversational format provided by the samsum dataset.
    meeting_conv = generate_dialogues(trans_dict["roles"], trans_dict["utterances"])

    # pratitioned document
    processed_dict = doc_partitioning(meeting_conv, max_characters=500)

    # apply summarizer to processed transcript
    output = apply_summarizer(processed_dict)
    output = split_Sentences(output)

    # generate different segments of the processed meeting
    person_List = generate_person_list(output)
    keywords_List = process_generated_keywords(process_code)
    attendees = generate_attendees(person_List) 
    keywords = generate_keywords(keywords_List)
    main_body_ = main_body(output)

    # Assemle parts and preparing final minute:
    DOCUMENT = prepare_document(attendees, keywords, main_body_)
    print(DOCUMENT)
    # TODO Change this later
    convert_str_2_txt(DOCUMENT, process_code)
  