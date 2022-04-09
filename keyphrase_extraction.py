import yake
import pandas as pd
import os

def process_keyword(process_code, path_to_transcripts_directory, path_to_keyword_directory, ngram=2):  
  language = "en"
  max_ngram_size = ngram
  deduplication_thresold = 0.9
  deduplication_algo = 'seqm'
  windowSize = ngram * 5
  numOfKeywords = 10
  custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_thresold, dedupFunc=deduplication_algo, windowsSize=windowSize, top=numOfKeywords, features=None)

  path_to_transcripts_directory = os.path.normpath(path_to_transcripts_directory) 
  filename = f"{process_code}.txt"
  path_to_processed_transcripts = os.path.join(path_to_transcripts_directory, filename)
  with open(path_to_processed_transcripts, 'r') as f:
    text_list = [line.strip() for line in f]
  # remove white space instances
  text_list = [instance for instance in text_list if instance is not ''] 
  generated_keywords = []
  for instance in text_list:
    keywords = custom_kw_extractor.extract_keywords(instance)
    for keyword in keywords:
      keyword, probability = keyword
      if probability > 0.1:
        generated_keywords.append(keyword)

  path_to_keyword_directory = os.path.normpath(path_to_keyword_directory)
  filename = f"{process_code}.csv"
  path_to_generated_keywords = os.path.join(path_to_keyword_directory, filename)
  # # save generated keyword as csv file
  keyword_df = pd.DataFrame({
      "text": generated_keywords
  })

  keyword_df['text'] = keyword_df.apply(
      lambda row: row['text'] if len(row['text'].split(' ')) >= ngram else float('NaN'),
      axis=1
  )
  keyword_df = keyword_df.dropna()
  keyword_df = keyword_df[:10]
  keyword_df.to_csv(path_to_generated_keywords, index=False)