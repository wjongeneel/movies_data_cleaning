# movies data cleaning
This is a practice project cleaning data from a csv (movies.csv) using pyspark and databricks. Data is cleaned and written to tables in databricks.

You can run the movies_data_pipeline.py notebook. This will run the other notebooks to import the data, clean the data, create tables and write the data to tables. The clean_movies_dataset.py file assumes that the movies.csv file has been stored on the following path in databricks: dbfs:/FileStore/movies.csv 

For this project I used Databricks community edition. 

#### link to original dataset
https://www.kaggle.com/datasets/bharatnatrayn/movies-dataset-for-feature-extracion-prediction
