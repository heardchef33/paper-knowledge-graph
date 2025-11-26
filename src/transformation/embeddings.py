import pandas as pd 
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.functions import pandas_udf

from sentence_transformers import SentenceTransformer, util

from clean import miscalleneous_cleaning
from config.spark_config import create_spark_session


## apply the sentence transformer 

## will the embedded column be a separate table (no - its a one-to-one relationship right)
## each research paper id has one embedded column

## convert the abstract column in series (not tokenised) each row has a abstract string 
## each string is a given a embedding which is return as a array of floats 

def to_series(df: DataFrame) -> pd.Series: 
    return df.select(
        F.col("abstract")
    ).toPandas()['abstract']

@pandas_udf("array<float>")
def embedder(abstract: pd.Series) -> pd.Series:
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(abstract.to_list(), show_progress_bar=True)
    return embeddings


if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    df = miscalleneous_cleaning(spark, PARQUET_FOLDER)

    to_series(df)


    
