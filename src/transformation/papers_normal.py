# normalise drop unnecessary columns and join the table with embeddings 
from pyspark.sql import functions as F

def paper_normalisation(spark, df, embedding_filepath): 
    """
    input: resulting dataframe from miscallenous df 
    drop columns that have already been normalised and join with the resulting table from embeddings 
    """

    print("dropping unnecessary columns")

    inter = df.drop(
        F.col("categories"), 
        F.col("journal-ref"), 
        F.col("authors_parsed_cleaned")
    )

    embeddings = spark.read.parquet(embedding_filepath)

    print("finish embeddings, joining")

    return inter.join(embeddings,
                      on="id",
                      how="inner")

def normalize_papers(df):
    """
    Simplified paper normalization without embeddings for faster loading
    Use this for bulk loading, then update with embeddings later
    """
    return df.select(
        F.col("id"),
        F.col("abstract"),
        F.col("title"),
        F.col("pub_year")
    ).dropDuplicates().dropna(subset=["id"])

if __name__ == "__main__": 

    from config.spark_config import create_spark_session

    from clean import miscalleneous_cleaning

    spark = create_spark_session()

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)

    EMBEDDING_FILEPATH = '/Users/thananpornsethjinda/Desktop/rkg/data/embeddings'

    paper_normalisation(spark=spark, df=df, embedding_filepath=EMBEDDING_FILEPATH)



