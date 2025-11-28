
from pyspark.sql import functions as F

from config.spark_config import create_spark_session

def categories_pairs(df):
    return df.select(
        "id",
        F.explode(F.col("categories")).alias("category")
    )

def category_normalisation(df, category_mapping): 
    return df.select(
        F.col("category")
    ).distinct().withColumn(
        "category_id", 
        F.md5(F.col("category"))
    ).join(category_mapping,
           on="category",
           how="inner")

def category_in_paper(category_pair_df, category_normal_df): 
    return category_pair_df.join(
        category_normal_df, 
        on="category", 
        how="inner"
    ).select("id", "category_id")  


if __name__ == "__main__":
    ...

    # PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    # spark = create_spark_session()

    # df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    # haha = categories_pairs(df)

    # normal = category_normalisation(haha)

    # category_in_paper(haha, normal)
