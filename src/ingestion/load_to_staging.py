from pyspark.sql import functions as F, types as T

from config.spark_config import create_spark_session

def read_raw(spark, input_path): 
    """
    read raw data and convert it to a pyspark dataframe 
    """
    print("Reading data ...")

    df = spark.read.json(input_path)

    df.printSchema()

    df.show(5)

    return df 

def create_partition_column(df): 
    """
    convert the update_date column to time stamp and extract the years
    """

    print("Partitioning ...")

    first_version_date = F.col("versions")[0]["created"]

    df_with_year = df.withColumn(
        "pub_year",
        F.regexp_extract(first_version_date, r'(\d{4})', 1).cast(T.IntegerType())
    )

    print("Success!")

    df_with_year.select("pub_year").show(5)

    return df_with_year

def convert_to_parquet(df, save_path):
    """
    save the partitioned data frame into parquet files 
    """   
    print("Saving to parquet files ...")  

    df.write.partitionBy("pub_year").mode("overwrite").parquet(save_path)

    print("Saving successful!")

def main():
    """
    main function for staging into parquet files
    """

    RAW_PATH = '/Users/thananpornsethjinda/Desktop/rkg/data/raw/arxiv-metadata-oai-snapshot.json'

    SAVE_PATH = '/Users/thananpornsethjinda/Desktop/rkg/data/staging'

    spark = create_spark_session()

    try: 

        print("Starting initial loading and staging process ...")
    
        df = read_raw(spark, RAW_PATH)

        df = create_partition_column(df)

        convert_to_parquet(df, SAVE_PATH)

        print("Staging completed successfully!")
    
    except Exception as e: 
        
        print(f"An unexpected error occurred: {e}")

    finally: 

        spark.stop()


if __name__ == "__main__": 

    main()






