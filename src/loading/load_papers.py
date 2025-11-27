from pyspark.sql import DataFrame
## create notes 

def load_papers_node(df: DataFrame):
    """
    take in the normalised paper dataframe and created nodes in neo4j
    """
    print("loading paper dataframe ...")
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("batch.size", "1000") \
        .option("labels", "Paper") \
        .option("node.keys", "id") \
        .save()
    print("loading complete")

def load_authors_node(df: DataFrame): 
    """
    take in the normalised author dataframe and created nodes in neo4j
    """
    print("loading author dataframe ...")
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("batch.size", "50000") \
        .option("labels", "Author") \
        .option("node.keys", "author_id")
    print("loading complete")


def load_category_node(df: DataFrame):
    """
    take in normalised concept data frame and create nodes
    """
    print("loading category dataframe ...")
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("batch.size", "50000") \
        .option("labels", "Category") \
        .option("node.keys", "category_id")
    print("loading complete")


def load_concept_node(df: DataFrame): 
    """
    take in normalised concept node df and create nodes
    """
    print("loading concept dataframe ...")
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("batch.size", "50000") \
        .option("labels", "Concept") \
        .option("node.keys", "concept_id")
    print("loading complete")


    





    