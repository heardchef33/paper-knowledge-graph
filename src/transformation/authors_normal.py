## get all names and deduplicate author names 
## normalisation of the authors 
## put it in a normalised form 

from pyspark.sql import SparkSession, functions as F, types as T

from pyspark import SparkConf

from pathlib import Path

import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_config

from clean import miscalleneous_cleaning

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

def author_normalisation(df): 
    """
    create a table with author id and all unique authors 
    """

    print("Exploding ...")

    df_exploded = df.select(

        F.explode(F.col("authors_parsed_cleaned")).alias("author_array")

    )

    df_exploded.show()

    print("Exploding complete")

    df_final = df_exploded.select(
        F.col("author_array").getItem(0).alias("last_name"),
        F.col("author_array").getItem(1).alias("first_name")
    )

    df_authors = df_final.withColumn("full_name",
        F.concat(
            F.col("first_name"),
            F.lit(" "),
            F.col("last_name")
        )
    ).drop("first_name", "last_name")
    
    df_authors_final = df_authors.withColumn("id", F.monotonically_increasing_id())

    df_authors_final.show()

    return df_authors

def author_deduplication(): 
    ...

if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    author_normalisation(df)



