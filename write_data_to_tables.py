# Databricks notebook source
from pyspark.sql.functions import explode
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window

# COMMAND ----------

w = Window().orderBy(lit("A"))

# title
df_title = df.select("movies").dropDuplicates()
df_title = df_title.withColumn("id", row_number().over(w))
df_title.select(["id", "movies"]).write.insertInto("title")

# COMMAND ----------

# genre
df_genre = df.select(explode(df["genre"])).dropDuplicates()
df_genre = df_genre.withColumn("id", row_number().over(w))
df_genre.select(["id", "col"]).write.insertInto("genre")

# COMMAND ----------

# star
df_star = df.select(explode(df["star"])).dropDuplicates()
df_star = df_star.withColumn("id", row_number().over(w))
df_star.select(["id", "col"]).write.insertInto("star")

# COMMAND ----------

# director
df_director = df.select(explode(df["director"])).dropDuplicates()
df_director = df_director.withColumn("id", row_number().over(w))
df_director.select(["id", "col"]).write.insertInto("director")

# COMMAND ----------

# movie
df_title = df_title.select("id", "movies")
df_title = df_title.withColumnRenamed("id", "title_id")
df_title = df_title.withColumnRenamed("name", "movies")

df_movie = df.select(
    "movies",
    "rating",
    "one_line",
    "votes",
    "start_year",
    "end_year",
    "star",
    "genre",
    "director",
)
df_movie = df_movie.join(df_title, "movies")
df_movie = df_movie.withColumn("id", row_number().over(w))

df_movie.select(
    "id", "title_id", "rating", "one_line", "votes", "start_year", "end_year"
).write.insertInto("movie")

# COMMAND ----------

# movie_to_star
df_movie_to_star = df_movie.select("id", explode(df["star"]))
df_movie_to_star = df_movie_to_star.withColumnRenamed("id", "movie_id")
df_movie_to_star = df_movie_to_star.withColumnRenamed("col", "star")

df_star = df_star.select("id", "col")
df_star = df_star.withColumnRenamed("id", "star_id")
df_star = df_star.withColumnRenamed("col", "star")

df_movie_to_star = df_movie_to_star.join(df_star, "star")
df_movie_to_star = df_movie_to_star.select("star_id", "movie_id").dropDuplicates()

df_movie_to_star.write.insertInto("movie_to_star")

# COMMAND ----------

# movie_to_genre
df_movie_to_genre = df_movie.select("id", explode(df["genre"]))
df_movie_to_genre = df_movie_to_genre.withColumnRenamed("id", "movie_id")
df_movie_to_genre = df_movie_to_genre.withColumnRenamed("col", "genre")

df_genre = df_genre.select("id", "col")
df_genre = df_genre.withColumnRenamed("id", "genre_id")
df_genre = df_genre.withColumnRenamed("col", "genre")

df_movie_to_genre = df_movie_to_genre.join(df_genre, "genre")
df_movie_to_genre = df_movie_to_genre.select("movie_id", "genre_id").dropDuplicates()

df_movie_to_genre.write.insertInto("movie_to_genre")

# COMMAND ----------

# movie_to_director
df_movie_to_director = df_movie.select("id", explode(df["director"]))
df_movie_to_director = df_movie_to_director.withColumnRenamed("id", "movie_id")
df_movie_to_director = df_movie_to_director.withColumnRenamed("col", "director")

df_director = df_director.select("id", "col")
df_director = df_director.withColumnRenamed("id", "director_id")
df_director = df_director.withColumnRenamed("col", "director")

df_movie_to_director = df_movie_to_director.join(df_director, "director")
df_movie_to_director = df_movie_to_director.select("director_id", "movie_id")

df_movie_to_director.write.insertInto("movie_to_director")
