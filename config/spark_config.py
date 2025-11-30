from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_config(): 
    """
    returns spark configuration for development based on my local machine 
    Optimized for bulk loading into Neo4j
    """
    return {
        "spark.app.name":"KnowledgeGraph", 
        "spark.master":"local[8]",
        "spark.driver.memory":"4g",
        "spark.executor.memory":"4g",
        "spark.driver.maxResultSize": "2g",  
        "spark.sql.shuffle.partitions":"16",
        "spark.default.parallelism":"16", 
        "spark.memory.fraction":"0.75",
        "spark.memory.storageFraction":"0.2",  
        "spark.network.timeout":"600s",
        "spark.executor.heartbeatInterval":"60s",
        "spark.serializer":"org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max":"512m",
        # neo4j connector added
        "spark.jars.packages": "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3",
        "neo4j.url": "bolt://localhost:7687",
        "neo4j.authentication.basic.username": "neo4j",
        "neo4j.authentication.basic.password": "your_password"
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