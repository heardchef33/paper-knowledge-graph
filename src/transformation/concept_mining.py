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

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, HashingTF
from pyspark.sql import functions as F 
from pyspark.sql import types as T


from config.spark_config import create_spark_session
from src.transformation.clean import miscalleneous_cleaning

import pandas as pd

def preprocess_token_abstract(df): 
    """
    1. remove punctuation
    2. remove special characters 
    3. remove symbols and stop words (by tokenising)
    4. convert to lowercase 
    """

    inter = df.select(
        F.col("id"),
        F.trim(F.lower(F.col("abstract"))).alias("inter_abstract")
    ).select(
        F.col("id").alias("paper_id"),
        F.regexp_replace(
            F.col("inter_abstract"), 
            r'[^a-zA-Z ]', 
            ''
        ).alias("inter_abstract")
    ) # could be an error 

    tokenizer = Tokenizer(inputCol="inter_abstract", outputCol="abstract_tokenised")

    remover = StopWordsRemover(
        inputCol="abstract_tokenised", 
        outputCol="processed_abstract"
    )

    cleaned = (remover.transform(tokenizer.transform(inter))).select(F.col("paper_id"), F.col("processed_abstract"))

    cleaned.show()

    return cleaned

    # do i remove numbers - yes


def tf_idf(abstract_processed_df): 
    """
    1. vectorisation (TF calculation)
    2. IDF calculation
    3. TF-IDF scoring 
    """
    print("finding tf-idf values ...")

    cv = CountVectorizer(inputCol="processed_abstract", outputCol="vectors", vocabSize=10000)
    cvModel = cv.fit(abstract_processed_df)

    tf_df = cvModel.transform(abstract_processed_df)

    idf = IDF(inputCol="vectors", outputCol="tf_idf_vec")
    idfModel = idf.fit(tf_df)
    tf_idf_df = idfModel.transform(tf_df)

    return tf_idf_df, cvModel.vocabulary

def top_n_concepts(tfidf_df, vocabulary, top_n=10):
    """
    extract top n concepts for each paper
    """
    def get_top_indices(vector, n=top_n):
        if vector is None:
            return []
        indices_scores = [(i, float(vector[int(i)])) for i in vector.indices]
        top = sorted(indices_scores, key=lambda x: x[1], reverse=True)[:n]
        return [(int(idx), score) for idx, score in top]
    
    top_udf = F.udf(get_top_indices, T.ArrayType(T.StructType([
        T.StructField("term_idx", T.IntegerType()),
        T.StructField("score", T.DoubleType())
    ])))
    
    with_top = tfidf_df.withColumn("top_concepts", top_udf(F.col("tf_idf_vec")))

    with_top.show()
    
    exploded = with_top.select(
        "paper_id",
        F.explode("top_concepts").alias("concept_data")
    ).select(
        "paper_id",
        F.col("concept_data.term_idx").alias("term_idx"),
        F.col("concept_data.score").alias("tfidf_score")
    )

    exploded.show()
    
    vocab_broadcast = spark.sparkContext.broadcast(vocabulary)
    
    def idx_to_word(idx):
        return vocab_broadcast.value[idx] if idx < len(vocab_broadcast.value) else None
    
    word_udf = F.udf(idx_to_word, T.StringType())

    exploded.withColumn("concept", word_udf(F.col("term_idx"))).select(
        "paper_id", "concept", "tfidf_score"
    ).show()
    
    return exploded.withColumn("concept", word_udf(F.col("term_idx"))).select(
        "paper_id", "concept", "tfidf_score"
    )

def create_concept_tables(has_concept_df):
    """
    Split into node and relationship tables.
    """
    # for (nodes)
    concepts_df = has_concept_df.select("concept").distinct().withColumn(
        "concept_id", F.md5(F.col("concept"))
    )
    
    # creating relationships (with IDs)
    relationships_df = has_concept_df.join(
        concepts_df, on="concept", how="inner"
    ).select("paper_id", "concept_id", "tfidf_score")
    
    return concepts_df, relationships_df


if __name__ == "__main__":

    # import numpy as np

    # PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    # spark = create_spark_session()

    # haha = miscalleneous_cleaning(spark, PARQUET_FOLDER)

    # lmao = preprocess_token_abstract(haha)

    # tfidf_df, vocabulary = tf_idf(lmao)

    # with_concepts = top_n_concepts(tfidf_df=tfidf_df, vocabulary=vocabulary)

    # print("n_concepts success")

    # cdf, rdf = create_concept_tables(with_concepts)

    # print("concept success")

    # cdf.show()

    # rdf.show()

    import numpy as np

    spark = create_spark_session()

    tfidf_df = spark.read.parquet('/Users/thananpornsethjinda/Desktop/rkg/data/concept_inspection')

    print("complete!")

    v = pd.read_csv('/Users/thananpornsethjinda/Desktop/rkg/data/vocab_inspection/vocab.csv', index_col=False)

    vocabulary = np.array(v['0'].dropna())

    with_concepts = top_n_concepts(tfidf_df=tfidf_df, vocabulary=vocabulary)

    cdf, rdf = create_concept_tables(with_concepts)

    print("complete sucess")

    cdf.show()

    rdf.show()

