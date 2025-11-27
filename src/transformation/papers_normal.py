# normalise drop unnecessary columns and join the table with embeddings 

from config.spark_config import create_spark_session
from pyspark.sql import functions as F
from clean import miscalleneous_cleaning

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

if __name__ == "__main__": 

    spark = create_spark_session()

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)

    EMBEDDING_FILEPATH = '/Users/thananpornsethjinda/Desktop/rkg/data/embeddings'

    paper_normalisation(spark=spark, df=df, embedding_filepath=EMBEDDING_FILEPATH)



