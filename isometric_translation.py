import boto3
from tqdm import tqdm
import subprocess
from transformers import MarianMTModel, MarianTokenizer

# utilities

#input: list of languages example : [fr,de]
#task: save translation files to meeting-minutes-translated folder in format "translated_fr_processcode"
  
def read_minute(path_to_file):
  with open(path_to_file, 'r') as f:
    minute = [line.strip() for line in f]  
  return minute

def translate_minutes(minute: list):
  translated_minutes = []
  for source_sentence in tqdm(minute, total=len(minute)):
    translated = model.generate(**tokenizer(source_sentence, return_tensors="pt", padding=True))  
    translated_minutes.extend([tokenizer.decode(t, skip_special_tokens=True) for t in translated])
  return translated_minutes

def save_2_text(translated_minutes: list, path_to_file: str):
  with open(path_to_file, 'w') as filehandle:
    for listitem in translated_minutes:
        filehandle.write('%s\n' % listitem)

model_checkpoints = "enimai/OPUS-mt-en-fr-finetuned-MUST-C"
tokenizer = MarianTokenizer.from_pretrained(model_checkpoints)
model = MarianMTModel.from_pretrained(model_checkpoints)

def generate_translated_document(process_code):
      
  # translate English minutes to French
  minute = read_minute(f"output/meeting-minutes/{process_code}.txt")
  translated_minutes = translate_minutes(minute)

  path_to_file = f"output/meeting-minutes-translated/translated_fr_{process_code}.txt"
  save_2_text(translated_minutes, path_to_file)

