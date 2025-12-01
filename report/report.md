# Research Knowledge Graph Process Blog

## Outline 

This blogs covers the process of the ideation and formation of the research knowledge graph, the problems faced and results. 
This blogs involves:
1. Information about the dataset and libraries used 
2. Transformation process of the data 
3. Loading process into Neo4j 
4. Results: Insights derived from the graph and graph algorithms like Page Rank
5. Problems faced 
6. Conclusion

## Data 

Data files used in this project is the arxiv-metadata file and the arxiv categories file 
The arxiv-metadata file contains meta-data for research papers ranging from 1997 to 2025 such as title, abstract, authors, categories, different versions, license and papers cited. 
The arxiv-categories file contain a mapping from the category code to the field name and the subcategory name 

The bulk of the data we have to process is in arxiv-metadata so the focus of the pipeline is too transformed this data. In particular, the table in metadata file is not normalised. Therefore, we have to change the raw table into an authors, category and paper tables that are normalised. The pairwise relationships between these 3 entities are all many to many relationships so we have to create joint tables to represent the relationships between these entities.

Apart from normalisation, we will also carry out text mining to extract the top 10 concepts from each paper so that these concepts can offer usefulness to users

For the official full pipeline, we can use `pyspark` to read this `json` file. However, for development and debugging purposes, the `json` file is converted to `parquet` file partitioned by year for columnar storage and fast reading. This was done by reading the file in spark and extracting the published year from the versions column resulting in the `pub_year` column containing the published year.

## Transformation 

### Normalisation 
As stated earlier the meta data table needs to be normalised since there are nested fields. 
Before normalising anything, we first did some cleaning and parsing of the data so it is manageable in future steps. In particular, we extracted out the author's name which was given in a list and also converted the string of categories separated by a space to an array of categories. We also dropped duplicate and null `id` values.

To normalise and create the papers table, we just have keep only the necessary columns in the raw table i.e `abstract, title, pub_year`.

To normalise the authors and categories table, we followed pretty much the same approach for both since the fields are nested in a similar manner. We first explode the author names to obtain a dataframe with `paper_id` and name of all authors (id-author pairs). Parse authors names and remove formatting errors eg. removing numbers and special characters and empty fields. To get the authors table, we select the authors column and get only distinct values and carry out md5 hashing on the author names to generate unique ids. To get the joint authors table, we carry out an inner join of the author table and the exploded table. 
For the categories table, we pretty much follow the same approach with the additional step of an inner join with the categories mapping table.

### Concept mining - This step involves extract useful words or "concepts" from abstracts 
This is done by carrying of term frequency-inverse document frequency (TF-IDF) which is basically a statistical measure of how important a word is to a document in a collection. 

Term Frequency: The total number of occurences a word appears in a document 
Inverse Document Frequency: Magnitude determining how rare or common a word is
The TF-IDF score is taking my multiplying TF and IDF for each word in the document abstract. The higher the score, the more important it is. 

The first step in concept mining is to select the `id` and the `abstract` column and carry preprocessing steps to such as removing numbers and special characters and converting everything to lowercase, removing stop words (common words with no meaning) and tokenising the text and filtering words that are greater than 1 character. 
This can be done using transformation commands in PySpark and models like `Tokenizer, StopWordsRemover` from the PySpark Machine Learning library. 

After pre-processing is complete, we select only nouns by using a pre-trained model from the `ntlk` library and `pandas udf` and then carry out the TF and IDF steps implicitly using the `CountVectorizer` and the `IDF` libraries. This results in a dataframe containing the `id`, the word or `concept` and the TF-IDF score for that concept. We then define a function to select the top n concepts. 

Similar as before, to get the normalised concept table, we select only the distinct concept values from concept column of the resulting table from TF-IDF and carry of md5 hashing for each one. To get the joint paper-concept table we join the normalised concept table and the resulting table from TF-IDF

## Loading Process 

For loading, the normalised dataframes are converted into csv files in the `data/neo4j_import`. We then follow the bulk csv load process stipulated by the neo4j documentation. 

We first: 
1. load all dataframes from memory into neo4j_import as csv files 
2. copy all the csv files to the import file in neo4j folder 
3. ensure that the docker container is down 
4. ensure that all database is empty by deleting all files in the database 
5. bulk load the csv files into the database 
6. after completion, activate the container 

The process until copying of csv files is included into the `main.py`
For deleting database files and bulk loading and interactions with docker we can write a bash script to carry this out 

## Results: Graph Insights and Algorithms 

### Basic Queries 

By traversing through nodes and observing through relationships between each node we can answer the following basic questions:

- What papers are in a category?
- What authors wrote this paper?
- What other papers has this author wrote?
- What other papers are in this subcategory or field? 

### Page Rank Algorithm

The page rank algorithm, originally, is an algorithm that measures the importance of a webpage based on the number and importance of the links point to it. We can apply it in the context of this research knowledge graph by treating the "webpages" as our target nodes.

Say we as the user is interested in reading software engineering related research papers but we perhaps don't know where to start; we can run a page rank algorithm to find the most "important" webpage 

## Problems Faced and Solutions 

1. Importing issues 
My initial approach for importing data was using a pyspark neo4j connector to import the dataframes directly from spark into neo4j. However, this process took extremely long; in fact it wasn't even successful importing one dataframe. This is possibly because spark had to re-combine all RDDs from each executor back to the driver node which took an extremely long time. 
Using bulk import using csv was significantly faster. 

2. Intial embedding approaches 
I also initially tried creating vector embeddings for each abstract using sentence transformers but convert each abstract took way too long. The vector embeddings could also not be joined with the normalised paper dataframe as it consistently caused memory issues. As a result, I removed the use of vector embeddings and focused on the accuracy of concept mining. 

## Improvements 
1. Include semantic vectors 
2. Citation data is not available; integrate citation from other data sources 