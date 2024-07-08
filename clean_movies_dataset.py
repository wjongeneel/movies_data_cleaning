# Databricks notebook source
# MAGIC %run ./pipeline_code/pipeline_code.py

movies_datasource = CsvDataSource(
    filepath="dbfs:/FileStore/movies.csv", header=True, multiline=True
)

df = movies_datasource.fetch_data()

class MoviesDataTransformer(DataTransformer):
    def clean_data(self):
        self.normalize_columns()
        self.drop_cols_with_high_nullperc(25)
        self.drop_rows_with_null_values()
        self.drop_duplicates()
        self.typecast_str("movies")
        self.strip_column("movies")
        self.extract_field_from_field("year", "start_year", "\((\d{4})")
        self.typecast_strnumber_to_int("start_year")
        self.extract_field_from_field("year", "end_year", "\(\d{4}–(\d{4})\)")
        self.typecast_strnumber_to_int("end_year")
        self.drop_column("year")
        self.strip_column("genre")
        self.split_col_by_delim("genre", ", ")
        self.typecast_strnumber_to_float("rating")
        self.strip_column("one_line")
        self.typecast_str("one_line")
        self.extract_field_from_field(
            "stars", "director", "Director[s]{0,}:(?<director>[\n\s\w\W]*?)\|"
        )
        self.regex_replace("director", "\\n", "")
        self.blank_as_null(["director"])
        self.split_col_by_delim("director", ", ")
        self.extract_field_from_field(
            "stars", "star", "Star[s]{0,}:\n+(?<star>[\w\s\W]{0,1000}+)"
        )
        self.regex_replace("star", "\\n", "")
        self.strip_column("star")
        self.blank_as_null(["star"])
        self.split_col_by_delim("star", ", ")
        self.drop_column("stars")
        self.typecast_strnumber_to_int("votes")
        self.blank_as_null()

movies_data_transformer = MoviesDataTransformer(df)
movies_data_transformer.clean_data()
df = movies_data_transformer.get_df()
