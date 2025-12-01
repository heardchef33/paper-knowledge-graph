# full pipeline
import subprocess

from pyspark.sql import functions as F
from config.spark_config import create_spark_session

from src.transformation.clean import miscalleneous_cleaning_full_pipeline, category_mappings
from src.transformation.papers_normal import normalise_papers
from src.transformation.authors_normal import extract_author_pairs, author_normalisation, author_wrote
from src.transformation.categories_normal import categories_pairs, category_normalisation, category_in_paper
from src.transformation.concept_mining import preprocess_token_abstract, tf_idf, top_n_concepts, create_concept_tables, extraction_nouns

from src.loading.prep_nodes import prepare_papers_csv, prepare_authors_csv, prepare_categories_csv, prepare_concepts_csv
from src.loading.prep_rs import prepare_contains_csv, prepare_has_concept_csv, prepare_wrote_csv

def main():
    """
    whole pipeline 
    1. clean
    2. normalise all dataframes 
    3. convert all relevant dataframes to csv 
    4. run bash import script 
    """
    json_data_path = "/Users/thananpornsethjinda/Desktop/rkg/data/raw/arxiv-metadata-oai-snapshot.json"
    jsonl_path = "/Users/thananpornsethjinda/Desktop/rkg/data/raw/arxiv-categories.jsonl"
    output_dir = "/Users/thananpornsethjinda/Desktop/rkg/data/neo4j_import"
    
    print("COMPLETE PIPELINE")
    print()

    spark = create_spark_session()

    # read json file and convert to parquet 
    
    #1
    print("step 1: clean raw data...")
    cleaned_df = miscalleneous_cleaning_full_pipeline(spark, json_data_path)
    print("cleaning done successfully!")
    
    #2
    print("step 2: normalising papers...")
    papers_df = normalise_papers(cleaned_df)
    print("paper normalisation done successfully!")
    
    #3
    print("step 3: normalising authors...")
    author_pairs_df = extract_author_pairs(cleaned_df)
    authors_df = author_normalisation(author_pairs_df)
    print("author normalisation done successfully!")

    wrote_df = author_wrote(author_pairs_df, authors_df)
    print("successfully created WROTE relationships!")
    
    #4
    print("step 4: normalising categories...")
    category_map = category_mappings(spark, jsonl_path)
    category_pairs_df = categories_pairs(cleaned_df)
    categories_df = category_normalisation(category_pairs_df, category_map)
    print("category normalisation done successfully!")
    
    contains_df = category_in_paper(category_pairs_df, categories_df)
    print("successfully created CONTAINS relationships!")

    #5
    print("step 5: extracting concepts using TF-IDF, finding top n and normalising...")
    
    processed_abstracts_df = preprocess_token_abstract(cleaned_df)
    print("preprocessing and tokenisation of abstracts successful!")

    processed_abstracts_df = extraction_nouns(processed_abstracts_df)
    
    tfidf_df, vocabulary = tf_idf(processed_abstracts_df)
    print("calculated TF-IDF scores")
    
    with_concepts_df = top_n_concepts(spark, tfidf_df=tfidf_df, vocabulary=vocabulary, top_n=10)
    print("paper concept pairs generated successfully!")
    
    concepts_df, has_concept_df = create_concept_tables(with_concepts_df)
    print("concept normalisation done successfully!")
    print("successfully created HAS_CONCEPT relationships!")
    
    #6
    print("step 6: save to CSV for neo4j-admin import...")
    
    prepare_papers_csv(papers_df, f"{output_dir}/papers")
    prepare_authors_csv(authors_df, f"{output_dir}/authors")
    prepare_categories_csv(categories_df, f"{output_dir}/categories")
    prepare_concepts_csv(concepts_df, f"{output_dir}/concepts")
    prepare_wrote_csv(wrote_df, f"{output_dir}/wrote")
    prepare_contains_csv(contains_df, f"{output_dir}/contains")
    prepare_has_concept_csv(has_concept_df, f"{output_dir}/has_concept")
    
    spark.stop()

    #7 
    print("step 7: run bash import script")
    ## to write bash import script 
    subprocess.run(["sh", "/Users/thananpornsethjinda/Desktop/rkg/src/loading/bulk_import.sh"])


if __name__ == "__main__":
    
    import time
    start = time.time()
    main()
    elapsed = time.time() - start
    print(f"Completed in {elapsed}")

    
    