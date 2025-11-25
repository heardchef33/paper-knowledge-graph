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

def extract_author_pairs(df): 
    """
    helper function to prevent repeats and redundancy in both functions (allows us to remove duplicate logic)
    1. explode all authors while retaining paper id and pub year (for checking)
    2. first later of filtering of bad author recrods 
    3. creating full_name column and apply 2nd level of filtering 
    """
    return df.select(
        "id",
        "pub_year",
        F.explode(F.col("authors_parsed_cleaned")).alias("author_array"),
    ).filter(
        (F.col("author_array").getItem(0) != '') & (F.col("author_array").getItem(1) != '') &
        (F.col("author_array").getItem(0).rlike("[a-zA-Z]")) & (F.col("author_array").getItem(1).rlike("[a-zA-Z]"))
    ).select(
        F.col("id").alias("paper_id"),
        F.concat(
            F.col("author_array").getItem(1),
            F.lit(" "),
            F.col("author_array").getItem(0)
        ).alias("inter_full_name"), 
        F.regexp_replace(
            "inter_full_name",
            r'^.*?([A-Z])',
            r'$1'
        ).alias("full_name")
    ).show()

def author_normalisation(df): 
    """
    create a table with author id and all unique authors 
    """

    print("Exploding ...")

    df_exploded = df.select(
        F.explode(F.col("authors_parsed_cleaned")).alias("author_array"),
    )

    # df_exploded.show()

    # print(df_exploded.count())

    print("Exploding complete")

    df_valid = df_exploded.filter(
        (F.col("author_array").getItem(0) != '') & (F.col("author_array").getItem(1) != '') &
        (F.col("author_array").getItem(0).rlike("[a-zA-Z]")) & (F.col("author_array").getItem(1).rlike("[a-zA-Z]"))
    )
    # df_valid.show()

    # print(df_valid.count())

    df_final = df_valid.select(
        F.col("author_array").getItem(0).alias("last_name"),
        F.col("author_array").getItem(1).alias("first_name")
    )

    df_authors = df_final.withColumn("full_name",
        F.concat(
            F.col("first_name"),
            F.lit(" "),
            F.col("last_name")
        )
    ).withColumn("compared", 
        F.regexp_replace(
            "full_name",
            r'^.*?([A-Z])',
            r'$1'
        )
    ).drop("first_name", "last_name").distinct()

    df_authors_final = df_authors.withColumn("author_id", F.md5(F.col("full_name")))

    return df_authors_final

def author_wrote(df, authors_df):
    
    """
    get the author wrote joint table by joining author table? 
    """ 

    # avoid calculating author normalisation again 

    print("Exploding ...")

    df_exploded = df.select(
        "id",
        "pub_year",
        F.explode(F.col("authors_parsed_cleaned")).alias("author_array"),
    )

    # df_exploded.show()

    # print(df_exploded.count())

    print("Exploding complete")

    df_valid = df_exploded.filter(
        (F.col("author_array").getItem(0) != '') & (F.col("author_array").getItem(1) != '') &
        (F.col("author_array").getItem(0).rlike("[a-zA-Z]")) & (F.col("author_array").getItem(1).rlike("[a-zA-Z]"))
    )
    # df_valid.show()

    # print(df_valid.count())

    df_final = df_valid.select(
        "id",
        "pub_year",
        F.col("author_array").getItem(0).alias("last_name"),
        F.col("author_array").getItem(1).alias("first_name")
    )

    df_authors = df_final.withColumn("full_name",
        F.concat(
            F.col("first_name"),
            F.lit(" "),
            F.col("last_name")
        )
    ).withColumn("compared", 
        F.regexp_replace(
            "full_name",
            r'^.*?([A-Z])',
            r'$1'
        )
    ).drop("first_name", "last_name").distinct()

    final = df_authors.join(authors_df, on="full_name", how="inner")

    final.show()

    ## function okay for now: but it contains alot of repeats - optimisation needed 

    return final 
    

if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    extract_author_pairs(df=df)



