
from config.spark_config import create_spark_session
from sentence_transformers import SentenceTransformer


## apply the sentence transformer 

## will the embedded column be a separate table (no - its a one-to-one relationship right)
## each research paper id has one embedded column

## convert the abstract column in series (not tokenised) each row has a abstract string 
## each string is a given a embedding which is return as a array of floats 

# def to_series(df: DataFrame) -> pd.Series: 
#     return df.select(

if __name__ == "__main__":

    spark = create_spark_session()

    model = SentenceTransformer('all-MiniLM-L6-v2') 

    for year in range(1991, 2026):
        year_df = spark.read.parquet(f"data/staging/pub_year={year}")
        year_pd = year_df.select("id", "abstract").toPandas()
        
        # generate embeddings
        embeddings = model.encode(year_pd['abstract'].tolist(), batch_size=32)
        year_pd['embedding'] = embeddings.tolist()
        
        # convert back to parquet to reduce memory contraint 
        year_pd[['id', 'embedding']].to_parquet(f"data/embeddings/{year}_embeddings.parquet")
        
        print(f"Year {year} complete")
    




    
