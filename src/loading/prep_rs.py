from pyspark.sql import functions as F

def prepare_wrote_csv(wrote_df, output_path):
    """
    prepare WROTE relationships with neo4j-admin import format
    header: :START_ID(Paper), :END_ID(Author), :TYPE
    """
    print("preparing WROTE relationships for neo4j-admin import...")
    
    wrote_clean = wrote_df.select(       
        F.col("author_id").alias(":START_ID(Author)"),
        F.col("paper_id").alias(":END_ID(Paper)"),
        F.lit("WROTE").alias(":TYPE")
    )
    
    wrote_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

    print("WROTE relationships converted to csv")

def prepare_contains_csv(contains_df, output_path):
    """
    prepare CONTAINS relationships with neo4j-admin import format
    header: :START_ID(Paper), :END_ID(Category), :TYPE
    """
    print("Preparing CONTAINS relationships for neo4j-admin import...")
    
    contains_clean = contains_df.select(
        F.col("category_id").alias(":START_ID(Category)"),
        F.col("id").alias(":END_ID(Paper)"),
        F.lit("CONTAINS").alias(":TYPE")
    )
    
    contains_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

    print("CONTAINS relationships converted to csv")

def prepare_has_concept_csv(has_concept_df, output_path):
    """
    prep HAS_CONCEPT relationships with neo4j-admin import format
    header: :START_ID(Paper), :END_ID(Concept), tfidf_score, :TYPE
    """
    print("Preparing HAS_CONCEPT relationships for neo4j-admin import...")
    
    has_concept_clean = has_concept_df.select(
        F.col("paper_id").alias(":START_ID(Paper)"),
        F.col("concept_id").alias(":END_ID(Concept)"),
        F.col("tfidf_score"),
        F.lit("HAS_CONCEPT").alias(":TYPE")
    )
    
    has_concept_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

    print("HAS_CONCEPT relationships converted to csv")

    
    