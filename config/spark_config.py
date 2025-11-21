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

