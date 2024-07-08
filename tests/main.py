from pipeline_code import CsvDataSource, DataTransformer
import pyspark
from pyspark.sql import SparkSession
import pytest
from pyspark.testing.utils import assertDataFrameEqual

spark = SparkSession.builder.appName("unit_tests").getOrCreate()


def test_CsvDataSource_fetch_data():
    expected_columns = [
        "MOVIES",
        "YEAR",
        "GENRE",
        "RATING",
        "ONE-LINE",
        "STARS",
        "VOTES",
        "RunTime",
        "Gross",
    ]
    expected_df_len = 3
    csv_data_source = CsvDataSource(
        "tests/test_data/CsvDataSource.csv", multiline=True, header=True
    )
    df = csv_data_source.fetch_data()
    actual_columns = df.columns
    actual_df_len = df.count()
    assert expected_columns == actual_columns
    assert expected_df_len == actual_df_len


def test_DataTransformer_get_df():
    dummy_df = spark.createDataFrame(
        [
            (1, "foo"),  # create your data here, be consistent in the types.
            (2, "bar"),
        ],
        ["id", "label"],  # add your column names here
    )
    csv_data_source = CsvDataSource(
        "tests/test_data/CsvDataSource.csv", multiline=True, header=True
    )
    df = csv_data_source.fetch_data()
    datatransformer = DataTransformer(df)
    actual_return_type = type(datatransformer.get_df())
    assert actual_return_type == type(dummy_df)


def test_DataTransformer_normalize_columns():
    expected_columns = [
        "movies",
        "year",
        "genre",
        "rating",
        "one_line",
        "stars",
        "votes",
        "runtime",
        "gross",
    ]
    csv_data_source = CsvDataSource(
        "tests/test_data/CsvDataSource.csv", multiline=True, header=True
    )
    df = csv_data_source.fetch_data()
    datatransformer = DataTransformer(df)
    datatransformer.normalize_columns()
    actual_columns = datatransformer.get_df().columns
    assert expected_columns == actual_columns
