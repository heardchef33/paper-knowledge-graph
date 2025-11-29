# full pipeline
from pyspark.sql import functions as F
from config.spark_config import create_spark_session

from src.transformation.clean import miscalleneous_cleaning, category_mappings
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
    parquet_path = "/Users/thananpornsethjinda/Desktop/rkg/data/staging"
    jsonl_path = "/Users/thananpornsethjinda/Desktop/rkg/data/raw/arxiv-categories.jsonl"
    output_dir = "/Users/thananpornsethjinda/Desktop/rkg/data/neo4j_import"
    
    print("COMPLETE PIPELINE")
    print()
    
    