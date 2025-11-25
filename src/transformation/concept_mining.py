"""
Notes from research paper: 
Process of keyword extraction system will look something like this: 
for each document 
1. preprocess - remove punctuation, special characters, symbols and stop words, convert to lower case 
    - rough approach: keep a dictionary of stop words 
2. tokenisation - split each abstract into a list of words .
    - rough approach: .split()
3. bag of words - list of words that are not repeated (set)
4. calculate tf for word in bag of words (keep a dict of what word has what tf)
5. calculate idf for word in bag of words 
6. calculate tf-idf for each word in bag of words 
7. if a word has a tf-idf greater than average it is considered a key word
8. sort key words and find top N 
"""

# lets write this after lunch 

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql import functions as F

from config.spark_config import create_spark_session
from src.transformation.clean import miscalleneous_cleaning

def preprocess_token_abstract(df): 
    """
    1. remove punctuation
    2. remove special characters 
    3. remove symbols and stop words 
    4. convert to lowercase 
    """

    inter = df.select(
        F.trim(F.lower(F.col("abstract"))).alias("inter_abstract")
    ).select(
        F.regexp_replace(
            F.col("inter_abstract"), 
            r'[^a-zA-Z ]', 
            ''
        ).alias("inter_abstract")
    )

    tokenizer = Tokenizer(inputCol="inter_abstract", outputCol="abstract_tokenised")

    remover = StopWordsRemover(
        inputCol="abstract_tokenised", 
        outputCol="processed_abstract"
    )

    cleaned = (remover.transform(tokenizer.transform(inter))).select(F.col("processed_abstract"))

    cleaned.show()

    return cleaned

    # do i remove numbers - yes


def tf_idf(df): 
    ...

if __name__ == "__main__":

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    haha = miscalleneous_cleaning(spark, PARQUET_FOLDER)

    preprocess_token_abstract(haha)

