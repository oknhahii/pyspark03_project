import logging.config
import os
import get_env_variables as gav
from ingrest import  load_files
from udfs import count_the_word,get_target
from sentiment_analysis import get_sentiment_analysis
logger = logging.getLogger('Email_handling')
from load_data_to_postgresql import load_data_to_postgresql

def handle_email(spark):
   file_list = []
   for file in os.listdir(gav.src_unstruc):
      file_format = 'csv'
      if file.endswith('txt'):
         file_format = 'txt'
      file_path = os.path.join(gav.src_unstruc, file)
      file_list.append({
         'file_name': file,
         'file_path': file_path,
         'file_format': file_format
      })

   statistic_list =[]
   for i in file_list:
      rdd = load_files(spark, file_dir=i['file_path'], file_format=i['file_format'], header='N/A',inferSchema='N/A')

      num_words = count_the_word(rdd)
      target_object = get_target(rdd)
      level = get_sentiment_analysis(rdd)

      statistic_list.append((i['file_name'],num_words,target_object,level))

   st_rdd = spark.sparkContext.parallelize(statistic_list)
   df = st_rdd.toDF(['file_name','num_words','target_object','level'])
   df.show()
   load_data_to_postgresql(df, 'sta_email')














