# Databricks notebook source
# read csv into dataframe
df = read_csv("dbfs:/FileStore/movies.csv", header=True, multiLine=True)


# COMMAND ----------

# replace - chars in column names with _ and set column names lowercase
df = normalize_column_names(df)

# COMMAND ----------

# drop dolumns with a percentage null values > 25 %
df = drop_cols_with_high_nullperc(df, 25)

# COMMAND ----------

# drop rows containing null values
df = drop_rows_with_null_values(df)

# COMMAND ----------

# drop duplicate rows
df = drop_duplicates(df)

# COMMAND ----------

# make sure column movies is of type str
df = typecast_str(df, "movies")

# make sure unnecessary spaces and newlines are stripped from movies column
df = strip_column(df, "movies")

# COMMAND ----------

# extract start_year from year
df = extract_field_from_field(df, "year", "start_year", "\((\d{4})")

# make sure start_year is of type int
df = typecast_strnumber_to_int(df, "start_year")

# extract end_year from year
df = extract_field_from_field(df, "year", "end_year", "\(\d{4}–(\d{4})\)")

# make sure end_year is of type int
df = typecast_strnumber_to_int(df, "end_year")

# drop original year column
df = drop_column(df, "year")

# COMMAND ----------

# make sure unnecessary spaces and newlines are stripped from genre column
df = strip_column(df, "genre")

# split genre row to array genre by delimiter ', '
df = split_col_by_delim(df, "genre", ", ")

# COMMAND ----------

# typecast rating column to float
df = typecast_strnumber_to_float(df, "rating")

# COMMAND ----------

# make sure unnecessary spaces and newlines are stripped from one_line column
df = strip_column(df, "one_line")

# typecast one_line column to str
df = typecast_str(df, "one_line")

# COMMAND ----------

# extract director part from stars column into director column
df = extract_field_from_field(
    df, "stars", "director", "Director[s]{0,}:(?<director>[\n\s\w\W]*?)\|"
)

# remove newlines from director column
df = df.withColumn("director", regexp_replace("director", "\\n", ""))

# fillnull column director
df = blank_as_null(df, ["director"])

# split director into director array on delim ', '
df = split_col_by_delim(df, "director", ", ")


# extract star part from stars column into star column
df = extract_field_from_field(
    df, "stars", "star", "Star[s]{0,}:\n+(?<star>[\w\s\W]{0,1000}+)"
)

# remove newlines from star column
df = df.withColumn("star", regexp_replace("star", "\\n", ""))

# strip star column from whitespaces at beginning and end
df = strip_column(df, "star")

# fillnull column star
df = blank_as_null(df, ["star"])

# split star into star array on delim ', '
df = split_col_by_delim(df, "star", ", ")

# drop stars column
df = drop_column(df, "stars")

# COMMAND ----------

# remove ',' char from number and typecast to int
df = typecast_strnumber_to_int(df, "votes")

# COMMAND ----------

# set all blank string column values as None
df = blank_as_null(df)
