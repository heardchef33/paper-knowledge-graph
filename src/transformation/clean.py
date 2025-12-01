from pyspark.sql import functions as F, types as T

from config.spark_config import create_spark_session

def miscalleneous_cleaning_full_pipeline(spark, file_path):
    """
    dropping unnecessary columns and preparing 
    author and categories for normalisation (full pipeline - no need for unnecessary conversion to parquet)
    """

    print("Reading generated parquet files in parallel ...")

    df = spark.read.json(file_path)

    print("Data successfully read")

    first_version_date = F.col("versions")[0]["created"]

    df = df.withColumn(
        "pub_year",
        F.regexp_extract(first_version_date, r'(\d{4})', 1).cast(T.IntegerType())
    )

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

    return authors_parsed.dropDuplicates(["id"]).dropna(subset=['id'])

def miscalleneous_cleaning(spark, parquet_file_path):
    """
    dropping unnecessary columns and preparing 
    author and categories for normalisation (for debugging)
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

    return authors_parsed.dropDuplicates(["id"]).dropna(subset=['id'])

def category_mappings(spark, json1_file_path):

    print("Reading category mappings")

    df = spark.read.json(json1_file_path)

    return df
