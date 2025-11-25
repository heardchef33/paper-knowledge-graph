
from pyspark.sql import SparkSession, functions as F, types as T

from pyspark import SparkConf

from pathlib import Path

import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_config

from clean import miscalleneous_cleaning

import hashlib

def create_spark_session(): # repeated for simplicity during development; will be removed later 
    """
    create spark session for development 
    """
    config = get_spark_config()

    conf = SparkConf()

    for con, settings in config.items(): 
        conf.set(con, settings)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark

def categories_pairs(df):
    return df.select(
        "id",
        F.explode(F.col("categories")).alias("category")
    )

def category_normalisation(df): 
    return df.select(
        F.col("category")
    ).distinct().withColumn(
        "category_id", 
        F.md5(F.col("category"))
    )

def category_in_paper(category_pair_df, category_normal_df): 
    return category_pair_df.join(
        category_normal_df, 
        on="category", 
        how="inner"
    ).show()


if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    haha = categories_pairs(df)

    normal = category_normalisation(haha)

    category_in_paper(haha, normal)
