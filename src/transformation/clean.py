from pyspark.sql import functions as F

from config.spark_config import create_spark_session

from clean import miscalleneous_cleaning

def miscalleneous_cleaning(spark, parquet_file_path):
    """
    dropping unnecessary columns and preparing 
    author and categories for normalisation
    """

    print("Reading generated parquet files in parallel ...")

    df = spark.read.parquet(parquet_file_path)

    print("Data successfully read")

    # drop irrelevant columnns (drooped version because we already have published year and submitter because people usually query authors)

    relevant_df = df.drop("authors", "comments", "doi", "license", "report-no", "update_date", "versions", "submitter")

    # proper parsing of categories

    categories_column = F.col('categories')

    cat_parsed = relevant_df.withColumn(
        'categories', 
        F.split(categories_column, " ")
    )

    # proper parsing of authors into a list of names

    authors_parsed = cat_parsed.withColumn(
        "authors_parsed_cleaned",
        F.transform(
            F.col("authors_parsed"),
            lambda author_array: F.slice(author_array, 1, 2)
            )
        ).drop("authors_parsed")

    authors_parsed.show()

    return authors_parsed

if __name__ == "__main__": 

    PARQUET_FOLDER = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    miscalleneous_cleaning(spark, PARQUET_FOLDER)