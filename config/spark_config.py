from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_config(): 
    """
    returns spark configuration for development based on my local machine 
    """
    return {
        "spark.app.name":"KnowledgeGraph", 
        "spark.master":"local[8]",
        "spark.driver.memory":"3g",
        "spark.executor.memory":"3g",
        "spark.sql.shuffle.partitions":"8",
        "spark.default.parallelism":"16", 
        "spark.driver.maxResultSize": "2g"
    }

def create_spark_session(): # repeated for simplicity during development; will be removed later 
    """
    create spark session for development 
    """
    config = get_spark_config()

    conf = SparkConf()

    for con, settings in config.items(): 
        conf.set(con, settings)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark