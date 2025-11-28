
from pyspark.sql import functions as F

from config.spark_config import create_spark_session

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

def normalize_categories(df):
    """Convenience function to get normalized categories from main dataframe"""
    category_pairs = categories_pairs(df)
    return category_normalisation(category_pairs)

def category_in_paper(category_pair_df, category_normal_df): 
    """Returns paper-category relationship dataframe"""
    return category_pair_df.join(
        category_normal_df, 
        on="category", 
        how="inner"
    ).select("id", "category_id")  # Fixed: removed .show() to return DataFrame


if __name__ == "__main__":
    ...

    # PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    # spark = create_spark_session()

    # df = miscalleneous_cleaning(spark, PARQUET_FOLDER)
    
    # haha = categories_pairs(df)

    # normal = category_normalisation(haha)

    # category_in_paper(haha, normal)
