from pyspark.sql import SparkSession, functions as F, types as T

from pyspark import SparkConf

from pathlib import Path

import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_config


def create_spark_session(): 
    """
    create spark session for development 
    """
    config = get_spark_config()

    conf = SparkConf()

    for con, settings in config.items(): 
        conf.set(con, settings)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark

def read_raw(spark, input_path): 
    """
    read raw data and convert it to a pyspark dataframe 
    """
    print("Reading data ...")

    df = spark.read.json(input_path)

    df.printSchema()

    return df 

def convert_to_parquet(spark, df, save_path):
    """
    partition data (research papers) by year and save them into parquet files 
    """     

    ...

if __name__ == "__main__": 

    RAW_PATH = '/Users/thananpornsethjinda/Desktop/rkg/data/raw/arxiv-metadata-oai-snapshot.json'

    spark = create_spark_session()

    read_raw(spark, RAW_PATH)





