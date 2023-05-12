# Databricks notebook source
'''
Budowa paczki:
za pomocą komendy "cd" w pythonowym terminalu przeniosłem się do miejsca gdzie stworzyłem folder z zawartością:
setup.py
setup.cfg
README.md
LICENSE.txt
KaggleToDatabricks
-__innit__.py
-Authenticate.py
-test_Authenticate.py
-TransformDataset.py
-test_TranfsformDataset.py

następnie przy pomocy komendy "python setup.py sdist" stworzyłem dystrybucję paczki
ostatecznie przy pomocy komendy "twine upload dist/*" i podając swój login i hasło wysłałem paczkę do pypi.orgazure

po kilku minutach można ją pobrać przy pomocy komendy "pip install"
'''

# COMMAND ----------

pip install KaggleToDatabricks==0.10.2

# COMMAND ----------

# Test którego nie udało mi się uruchomić z poziomu paczki
# pierwsze odpalenie pokazuje 0 testów, kolejne poprawnie 1 test
import csv
import os
import unittest
import pandas as pd
from KaggleToDatabricks import TransformDataset

TD = TransformDataset.TransformDataset
test_file = '/dbfs/FileStore/Kaggle_Datasets/test.csv'
rows = [
    ['0a', '0b', '0c'],
    ['1a', '1b', '1c'],
]


class TestTransformDataset(unittest.TestCase):

    import pandas as pd

    def setUp(self):
        """
        Creating dummy csv file to test on
        """
        with open(test_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(rows)

    def tearDown(self):
        """
        clearing dummy csv file after test
        """
        os.remove(test_file)

    def test_df_from_dataset(self):
        """
        testing df_from_dataset using dummy csv file created from test_file and rows variables
        """
        result = TD.df_from_dataset(self, "/dbfs/FileStore/Kaggle_Datasets", "test.csv")
        self.assertIsInstance(result, pd.DataFrame, "hej hej")

    if __name__ == '__main__':
        unittest.main(argv=['first-arg-is-ignored'], exit=False)


# COMMAND ----------

# Declaring variables
from py4j.protocol import Py4JJavaError # Importing Error object for precise exception catching
ingest_flag = False # this flag indicates if we have local csv file or need to ingest it from source
dataset_file = 'GlobalLandTemperaturesByState.csv'
# If you run this notbook without "input_path" parameter it will download dataset from task automatically using personal API key.
try:
    input_path = dbutils.widgets.get("input_path") 
except Py4JJavaError as e:

    print(f"parameter not provided. Ingesting dataset automatically using API")    
    ingest_flag = True
      

try:
    output_path = dbutils.widgets.get("output_path")
except Py4JJavaError as e:

    output_path = '/FileStore/Kaggle_Datasets/TemperaturesByState'
    print(f"parameter not provided. Using default parameter \"{output_path}\"")

# COMMAND ----------

if ingest_flag == True:

    #Setting variables for ingestion process

    json_file_path = '/FileStore/Kaggle_Token/arturwoniak.json' 
    
    with open("/dbfs/FileStore/Kaggle_Token/arturwoniak.json","w") as f:
        # This will create a private API key that should never be stored there or created that way!!! In normal conditions I would place it in key vault
        f.write(r'{"username":"arturwoniak","key":"a10d4f40825e4f883a990ffbd17faee4"}') 

    input_path = "/FileStore/Raw_Kaggle_Datasets/TemperaturesByState"
    dataset_name = 'berkeleyearth/climate-change-earth-surface-temperature-data/GlobalLandTemperaturesByState.csv'
    

    spark_json_df = spark.read.format('json').option('header','true').option('inferschema','true').load(json_file_path) # Reading json api config

    KAGGLE_USERNAME = spark_json_df.select(spark_json_df.username).take(1)[0]['username']
    KAGGLE_KEY = spark_json_df.select(spark_json_df.key).take(1)[0]['key']  

    #Authenticate using Kaggle API

    from KaggleToDatabricks import Authenticate 
    A = Authenticate.Authenticate
    api = A.authenticate_kaggle(A,KAGGLE_USERNAME,KAGGLE_KEY)

    #downloading file to input path

    api.dataset_download_files(dataset_name,path=input_path,unzip=True,force=True)


# COMMAND ----------


from KaggleToDatabricks import TransformDataset
td = TransformDataset.TransformDataset
df = td.df_from_dataset(td,input_path,dataset_file)

col_list_to_drop = ['dt', 'AverageTemperatureUncertainty']
col_list_to_groupby = ['State','Country']
col_get_max = 'AverageTemperature'

df = td.filter_dataset(td,df,col_list_to_drop)
df = td.get_max_from_dataset(td,df,col_list_to_groupby,col_get_max)
td.write_df_to_parquet(td,df,output_path)
    

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE plsHireMe;
# MAGIC CREATE TABLE plsHireMe
# MAGIC USING parquet
# MAGIC OPTIONS (path "/FileStore/Kaggle_Datasets/TemperaturesByState")

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table plsHireMe

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from plsHireMe
# MAGIC where Country = "Brazil"
