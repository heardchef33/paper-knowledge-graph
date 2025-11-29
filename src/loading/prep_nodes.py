from pyspark.sql import functions as F

def prepare_papers_csv(papers_df, output_path):
    """
    prepare papers with neo4j-admin import format
    header: id:ID(Paper), abstract, title, pub_year, :LABEL
    """
    print("preparing paper nodes for neo4j-admin import...")
    
    papers_clean = papers_df.select(
        F.col("id").alias("id:ID(Paper)"),
        F.regexp_replace(F.col("abstract"), "[\r\n]+", " ").alias("abstract"),
        F.regexp_replace(F.col("title"), "[\r\n]+", " ").alias("title"),
        F.col("pub_year"),
        F.lit("Paper").alias(":LABEL")
    )
    
    papers_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

    print("paper df converted to csv for loading")

def prepare_authors_csv(authors_df, output_path):
    """
    prepare authors with neo4j-admin import format
    header: author_id:ID(Author), full_name_normalised, :LABEL
    """
    print("preparing Author nodes for neo4j-admin import...")
    
    authors_clean = authors_df.select(
        F.col("author_id").alias("author_id:ID(Author)"),
        F.regexp_replace(F.col("full_name_normalised"), "[\r\n]+", " ").alias("full_name_normalised"),
        F.lit("Author").alias(":LABEL")
    )
    
    authors_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

def prepare_categories_csv(categories_df, output_path):
    """
    prepare categories with neo4j-admin import format
    header: category_id:ID(Category), category, :LABEL
    """
    print("preparing category nodes for neo4j-admin import...")

    categories_clean = categories_df.select(
        F.col("category_id").alias("category_id:ID(Category)"),
        F.col("field"),
        F.col("subcategory"),
        F.regexp_replace(F.col("category"), "[\r\n]+", " ").alias("category"),
        F.lit("Category").alias(":LABEL")
    )

    categories_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )

def prepare_concepts_csv(concepts_df, output_path):
    """
    prepare concepts with neo4j-admin import format
    header: concept_id:ID(Concept), concept, :LABEL
    """
    print("Preparing Concept nodes for neo4j-admin import...")
    
    concepts_clean = concepts_df.select(
        F.col("concept_id").alias("concept_id:ID(Concept)"),
        F.regexp_replace(F.col("concept"), "[\r\n]+", " ").alias("concept"),
        F.lit("Concept").alias(":LABEL")
    )
    
    concepts_clean.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True,
        quote='"',
        escape='"',
        lineSep="\n"
    )
    

    
    