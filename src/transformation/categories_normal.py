from pyspark.sql import functions as F

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


