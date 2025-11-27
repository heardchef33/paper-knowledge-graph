def load_authors_wrote(df):
    """
    write relationship to database 
    author_wrote table contains:
    - paper_id
    - author_id
    """
    print("loading paper-author relationship")

    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("relationship", "WROTE") \
        .option("relationship.source.labels", "Author") \
        .option("relationship.source.keys", "author_id") \
        .option("relationship.target.labels", "Paper") \
        .option("relationship.target.keys", "id") \
        .save()

def load_contain_category(df): 
    """
    write relationships 
    category_in_paper contains: 
    - paper id 
    - concept id 
    """
    print("load paper-category relationship")

    df.write \
        .mode("overwrite") \
        .option("relationship", "CONTAINS") \
        .option("relationship.source.labels", "Category") \
        .option("relationship.source.keys", "category_id") \
        .option("relationship.target.labels", "Paper") \
        .option("relationship.target.labels", "id") \
        .save()

def load_contain_concept(df): 
    """
    write relationships 
    concept_in_paper contains: 
    - paper id 
    - concept id 
    """
    print("load paper-concept relationship")

    df.write \
        .mode("overwrite") \
        .option("relationship", "HAS") \
        .option("relationship.source.labels", "Papr") \
        .option("relationship.source.keys", "id") \
        .option("relationship.target.labels", "Concept") \
        .option("relationship.target.labels", "concept_id") \
        .save()





