## get all names and deduplicate author names 
## normalisation of the authors 
## put it in a normalised form 

from pyspark.sql import functions as F

from config.spark_config import create_spark_session

from clean import miscalleneous_cleaning

def extract_author_pairs(df): 
    """
    helper function to prevent repeats and redundancy in both functions (allows us to remove duplicate logic)
    1. explode all authors while retaining paper id and pub year (for checking)
    2. first later of filtering of bad author recrods a
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
    ).withColumn(
        "full_name_normalised",
        F.lower(F.trim(F.col("full_name")))
    ).select(
        "paper_id",
        "full_name_normalised"
    )

def author_normalisation(df): 
    """
    take in the extrac author pair df 
    create a table with author id and all unique authors 
    1. remove duplicates 
    3. generate unique ids using md5 
    """
    return df.select(
        "full_name_normalised"
    ).distinct().select(
        F.md5(F.col("full_name_normalised")).alias("author_id"),
        F.col("full_name_normalised")
    )


def author_wrote(authors_pair_df, authors_df):
    
    """
    get the author wrote joint table by joining author table? 
    """ 

    # avoid calculating author normalisation again 
    # simply join 

    return authors_pair_df.join(
        authors_df, 
        on = "full_name_normalised",
        how = "inner"
        )
    

if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    authors_pair_df = extract_author_pairs(df=df)

    authors_df = author_normalisation(authors_pair_df)

    author_wrote(authors_pair_df=authors_pair_df, authors_df=authors_df)



