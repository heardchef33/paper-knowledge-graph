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

def normalise_papers(df):
    
    return df.select(
        F.col("id"),
        F.col("abstract"),
        F.col("title"),
        F.col("pub_year")
    )



