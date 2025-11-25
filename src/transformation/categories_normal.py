
from pyspark.sql import functions as F

from config.spark_config import create_spark_session

from clean import miscalleneous_cleaning

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
