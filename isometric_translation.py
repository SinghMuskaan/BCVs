import os
import boto3
import shutil
# from tqdm.notebook import tqdm
from tqdm import tqdm
import pandas as pd
from torch.utils.data import DataLoader
from datasets import load_dataset
import transformers
import subprocess
from transformers import MarianMTModel, MarianTokenizer, AutoTokenizer, AutoModel, MBartForConditionalGeneration

tqdm.pandas()

# utilities
def read_minute(path_to_file):
  with open(path_to_file, 'r') as f:
    minute = [line.strip() for line in f]  
  return minute

def translate_minutes(
    minute: list, 
    tokenizer: transformers.models.marian.tokenization_marian.MarianTokenizer, 
    model: transformers.models.marian.modeling_marian.MarianMTModel
):
  translated_minutes = []
  for source_sentence in tqdm(minute, total=len(minute)):
    translated = model.generate(**tokenizer(source_sentence, return_tensors="pt", padding=True))  
    translated_minutes.extend([tokenizer.decode(t, skip_special_tokens=True) for t in translated])
  return translated_minutes

def save_2_text(translated_minutes: list, path_to_file: str):
  with open(path_to_file, 'w') as filehandle:
    for listitem in translated_minutes:
        filehandle.write('%s\n' % listitem)

def check_for_verbosity(input_text, target_text):
  if not input_text or not target_text:
      return False
  ts_ratio = len(target_text)/len(input_text)
  if not (ts_ratio >= 0.90 and ts_ratio <= 1.10):
    return True
  return False

def append_paraphrase_prompt(input_text, target_text):
  ts_ratio = len(target_text)/len(input_text)
  prefix = None
  if ts_ratio < 0.90:
    prefix = "paraphrase long"
  elif ts_ratio > 1.10:
    prefix = "paraphrase short"
  target_text = prefix + " " + target_text
  return target_text

# -------------------------------------------------------- #

language_2_mt_model_mappings = {
    'de': 'enimai/opus-mt-en-de-finetuned-en-to-de',
    'fr': 'enimai/OPUS-mt-en-fr-finetuned-MUST-C',
    'ru': 'enimai/opus-mt-en-ru-finetuned-en-to-ru', 
    'it': 'enimai/opus-mt-en-it-finetuned-en-to-it',
    'hi': 'enimai/opus-mt-en-hi-finetuned-en-to-hi',
}
language_2_para_model_mappings = {
    'de': 'enimai/mbart-large-50-paraphrase-finetuned-for-de',
    'fr': 'enimai/mbart-large-50-paraphrase-finetuned-for-fr',
    'ru': 'enimai/mbart-large-50-paraphrase-finetuned-for-ru'
}

def simple_translation(
    text: str, 
    tokenizer: transformers.models.marian.tokenization_marian.MarianTokenizer, 
    model: transformers.models.marian.modeling_marian.MarianMTModel
):
  output = []
  translated = model.generate(**tokenizer(text, return_tensors="pt", padding=True))  
  output.extend([tokenizer.decode(t, skip_special_tokens=True) for t in translated])
  return output

def translate_keywords(languages, process_code):
    current_directory = os.getcwd()
    path_to_output_folder = os.path.join(current_directory, "output")
    path_to_keywords_folder = os.path.join(path_to_output_folder, 'processed-keywords')
    path_to_translated_keywords_folder = os.path.join(path_to_output_folder, 'keywords-translated')
    path_to_keyword_file = [os.path.join(path_to_keywords_folder, file) for file in os.listdir(path_to_keywords_folder) if file == f"{process_code}.csv"][0]
    for language in languages:
      df = pd.read_csv(path_to_keyword_file)
      model_name = language_2_mt_model_mappings[language]
      tokenizer = MarianTokenizer.from_pretrained(model_name)
      model = MarianMTModel.from_pretrained(model_name)
      translated_texts = []
      for index, row in df.iterrows():
        translated_texts.extend(
            simple_translation(row['text'], tokenizer, model))
      translated_df = pd.DataFrame({'text': translated_texts})
      path_to_translated_keyword_file = os.path.join(path_to_translated_keywords_folder, f"translated_{language}_{process_code}.csv")
      translated_df.to_csv(path_to_translated_keyword_file, index=False)
    
def process_transcripts_translation(languages, process_code):
    current_directory = os.getcwd()
    path_to_output_folder = os.path.join(current_directory, "output")
    path_to_transcript_folder = os.path.join(path_to_output_folder, 'processed-transcripts')
    path_to_translated_transcripts_folder = os.path.join(path_to_output_folder, 'transcripts-translated')
    path_to_transcripts_file = [os.path.join(path_to_transcript_folder, file) for file in os.listdir(path_to_transcript_folder) if file == f"{process_code}.txt"][0]    
    for language in languages:
        transcripts = read_minute(path_to_transcripts_file)
        model_name = language_2_mt_model_mappings[language]
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)
        translated = translate_minutes(transcripts, tokenizer, model)
        path_to_translated_file = os.path.join(path_to_translated_transcripts_folder, f"translated_{language}_{process_code}.txt")
        save_2_text(translated, path_to_translated_file)    
    
def process_translation(languages, process_code):
    current_directory = os.getcwd()
    path_to_output_folder = os.path.join(current_directory, "output")
    path_to_minutes_folder = os.path.join(path_to_output_folder, 'meeting-minutes')
    path_to_translated_minutes_folder = os.path.join(path_to_output_folder, 'meeting-minutes-translated')
    path_to_minute_file = [os.path.join(path_to_minutes_folder, file) for file in os.listdir(path_to_minutes_folder) if file == f"{process_code}.txt"][0]    
    for language in languages:
        minutes = read_minute(path_to_minute_file)
        model_name = language_2_mt_model_mappings[language]
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)
        translated = translate_minutes(minutes, tokenizer, model)
        path_to_translated_file = os.path.join(path_to_translated_minutes_folder, f"translated_{language}_{process_code}.txt")
        save_2_text(translated, path_to_translated_file)

def process_paraphrase(languages, process_code):
    para_languages = list(language_2_para_model_mappings.keys())
    for language in languages:
        if language not in para_languages:
            continue

        current_directory = os.getcwd()
        path_to_output_folder = os.path.join(current_directory, "output")
        path_to_minutes_folder = os.path.join(path_to_output_folder, 'meeting-minutes')
        path_to_translated_minutes_folder = os.path.join(path_to_output_folder, 'meeting-minutes-translated')
        path_to_minute_file = [os.path.join(path_to_minutes_folder, file) for file in os.listdir(path_to_minutes_folder) if file == f"{process_code}.txt"][0]    
        path_to_translated_file = [os.path.join(path_to_translated_minutes_folder, file) for file in os.listdir(path_to_translated_minutes_folder) if file == f"translated_{language}_{process_code}.txt"][0]
        minutes = read_minute(path_to_minute_file)
        translated = read_minute(path_to_translated_file)

        model_name = language_2_para_model_mappings[language]
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = MBartForConditionalGeneration.from_pretrained(model_name)

        pred_df = pd.DataFrame({
            'input_text': minutes,
            'target_text': translated
        })

        # check if mt_prediction -> input length ratio is normal
        pred_df["is_normal"] = pred_df.progress_apply(
            lambda row: check_for_verbosity(row['input_text'], row['target_text']),
            axis=1
        )
        not_normal_seq_index = pred_df.index[pred_df['is_normal'] == True].to_list()
        columns = ["input_text", "target_text"]
        pred_normal_df = pred_df[~pred_df.index.isin(not_normal_seq_index)][columns]
        pred_not_normal_df = pred_df[pred_df.index.isin(not_normal_seq_index)][columns]

        # apply paraphrase prompt 
        pred_not_normal_df["target_text"] = pred_not_normal_df.progress_apply(
            lambda row: append_paraphrase_prompt(row['input_text'], row['target_text']),
            axis=1
        )

        # temp folder to store files
        path_to_temp_folder = os.path.join(current_directory, f"temp_{process_code}")
        if not os.path.exists(path_to_temp_folder):
            os.makedirs(path_to_temp_folder)


        path_to_not_normal_file = os.path.join(path_to_temp_folder, "test_not_normal.csv")
        path_to_normal_file = os.path.join(path_to_temp_folder, "test_normal.csv")
        pred_not_normal_df.to_csv(path_to_not_normal_file, index=False)
        pred_normal_df.to_csv(path_to_normal_file, index=False)

        processed_raw_test_dataset = load_dataset('csv', data_files={"test": path_to_not_normal_file})
        test_dataloader = DataLoader(processed_raw_test_dataset["test"], batch_size=1, num_workers=0)

        predictions = []
        for batch in tqdm(test_dataloader):
            translated = model.generate(**tokenizer(batch['target_text'], return_tensors="pt", padding=True))
            predictions.extend([tokenizer.decode(t, skip_special_tokens=True) for t in translated])

        pred_not_normal_df["target_text"] = predictions
        processed_pred_df = pd.concat([pred_normal_df, pred_not_normal_df]).sort_index()
        shutil.rmtree(path_to_temp_folder)

        processed_translated = processed_pred_df["target_text"].to_list()
        save_2_text(processed_translated, path_to_translated_file)
        
def process_transcripts_paraphrase(languages, process_code):
    para_languages = list(language_2_para_model_mappings.keys())
    for language in languages:
        if language not in para_languages:
            continue
            
        current_directory = os.getcwd()
        path_to_output_folder = os.path.join(current_directory, "output")
        path_to_transcript_folder = os.path.join(path_to_output_folder, 'processed-transcripts')
        path_to_translated_transcripts_folder = os.path.join(path_to_output_folder, 'transcripts-translated')    
        path_to_transcripts_file = [os.path.join(path_to_transcript_folder, file) for file in os.listdir(path_to_transcript_folder) if file == f"{process_code}.txt"][0]    
        path_to_translated_file = [os.path.join(path_to_translated_transcripts_folder, file) for file in os.listdir(path_to_translated_transcripts_folder) if file == f"translated_{language}_{process_code}.txt"][0]
       
      
        transcripts = read_minute(path_to_transcripts_file)
        translated = read_minute(path_to_translated_file)

        model_name = language_2_para_model_mappings[language]
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = MBartForConditionalGeneration.from_pretrained(model_name)

        pred_df = pd.DataFrame({
            'input_text': transcripts,
            'target_text': translated
        })

        # check if mt_prediction -> input length ratio is normal
        pred_df["is_normal"] = pred_df.progress_apply(
            lambda row: check_for_verbosity(row['input_text'], row['target_text']),
            axis=1
        )
        not_normal_seq_index = pred_df.index[pred_df['is_normal'] == True].to_list()
        columns = ["input_text", "target_text"]
        pred_normal_df = pred_df[~pred_df.index.isin(not_normal_seq_index)][columns]
        pred_not_normal_df = pred_df[pred_df.index.isin(not_normal_seq_index)][columns]

        # apply paraphrase prompt 
        pred_not_normal_df["target_text"] = pred_not_normal_df.progress_apply(
            lambda row: append_paraphrase_prompt(row['input_text'], row['target_text']),
            axis=1
        )

        # temp folder to store files
        path_to_temp_folder = os.path.join(current_directory, f"temp_{process_code}")
        if not os.path.exists(path_to_temp_folder):
            os.makedirs(path_to_temp_folder)


        path_to_not_normal_file = os.path.join(path_to_temp_folder, "test_not_normal.csv")
        path_to_normal_file = os.path.join(path_to_temp_folder, "test_normal.csv")
        pred_not_normal_df.to_csv(path_to_not_normal_file, index=False)
        pred_normal_df.to_csv(path_to_normal_file, index=False)

        processed_raw_test_dataset = load_dataset('csv', data_files={"test": path_to_not_normal_file})
        test_dataloader = DataLoader(processed_raw_test_dataset["test"], batch_size=1, num_workers=0)

        predictions = []
        for batch in tqdm(test_dataloader):
            translated = model.generate(**tokenizer(batch['target_text'], return_tensors="pt", padding=True))
            predictions.extend([tokenizer.decode(t, skip_special_tokens=True) for t in translated])

        pred_not_normal_df["target_text"] = predictions
        processed_pred_df = pd.concat([pred_normal_df, pred_not_normal_df]).sort_index()
        shutil.rmtree(path_to_temp_folder)

        processed_translated = processed_pred_df["target_text"].to_list()
        save_2_text(processed_translated, path_to_translated_file)

def generate_translated_document(languages, process_code, process_type="min"):
    if process_type == "min"
      # generate translation
      process_translation(languages=languages, process_code=process_code)  
      # rephrase translation for isometric   
      process_paraphrase(languages=languages, process_code=process_code)
    else: 
      # generate translation
      process_transcripts_translation(languages=languages, process_code=process_code)
      # rephrase translation for isometric
      process_transcripts_paraphrase(languages=languages, process_code=process_code)
