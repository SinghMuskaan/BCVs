import yake
import pandas as pd
import os

# keyword/phrase extraction
kw_extractor = yake.KeywordExtractor()

language = "en"
max_ngram_size = 5
deduplication_thresold = 0.9
deduplication_algo = 'seqm'
windowSize = 3
numOfKeywords = 50
custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_thresold, dedupFunc=deduplication_algo, windowsSize=windowSize, top=numOfKeywords, features=None)

# convert processed transcripts (.txt) to keywords (.csv)
def generate_keywords(process_code, path_to_transcripts_directory, path_to_keyword_directory, threshold=0.1):
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
  keyword_df.to_csv(path_to_generated_keywords, index=False)