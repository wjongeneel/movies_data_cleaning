# Databricks notebook source
from pyspark.sql.functions import when, regexp_extract, col, regexp_replace, trim, split
import pyspark.sql.utils

def blank_as_null(df: pyspark.sql.dataframe.DataFrame, column_array: list=None) -> pyspark.sql.dataframe.DataFrame:
    """ Replaces all empty string values in with None in columns of DataFrame

        Keyword Arguments: 
        df: DataFrame
        columns_array: list (optional, default=None) - if specified the function will apply to this columns, otherwise funtion will be applied to all columns

        Returns: 
        DataFrame 
    """ 
    if column_array == None: 
        for column in df.columns:
            if df.select(column).dtypes[0][1] == 'string':
                df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
        return df 
    for column in column_array:
        if df.select(column).dtypes[0][1] == 'string':
            df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
    return df 

def read_csv(filename: str, header: bool=True, multiLine: bool=False) -> pyspark.sql.dataframe.DataFrame:
    """ Reads a csv file into a DataFrame

        Keyword arguments:
        filename: str - path to csv file 
        header: bool - whether or not csv contains headers
        multiLine: bool - whether or not rows of csv span multiple lines

        Returns:
        DataFrame
    """
    try:
        return spark.read.csv(path=filename, header=header, multiLine=multiLine)
    except pyspark.sql.utils.AnalysisException:
        print(f'Could not open file {filename}, check if filepath exists')

def normalize_column_names(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """ Replaces '-' chars in column names with '_' chars
        Makes sure all column names are lowercase 

        Keyword arguments:
        df: DataFrame

        Returns:
        DataFrame
    """
    for name in df.schema.names: 
        df = df.withColumnRenamed(name, name.replace('-', ' ').lower())
    for name in df.schema.names: 
        df = df.withColumnRenamed(name, name.lower())
    for name in df.schema.names: 
        df = df.withColumnRenamed(name, name.replace(' ', '_'))
    return df 
    
def drop_rows_with_null_values(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame: 
    """ Drops rows from DataFrame that has null values in any column
        
        Keyword arguments:
        df: DataFrame

        Returns:
        DataFrame 
    """
    return df.na.drop()

def drop_duplicates(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame: 
    """ Drops duplicate rows from DataFrame
        
        Keyword arguments: 
        df: DataFrame 

        Returns:
        DataFrame
    """
    return df.dropDuplicates()


def extract_field_from_field(df: pyspark.sql.dataframe.DataFrame, src_field: str, dest_field: str, regex: str) -> pyspark.sql.dataframe.DataFrame:
    """ Extracts dest_field from src_field using a regex on src_field and adds the dest_field to the DataFrame

        Keyword arguments:
        df: DataFrame
        src_field: str - field on which the regex will run
        dest_field: str - field into which match will be stored
        regex: str - regex used to extract dest_field from src_field, the first capturing group will be saved to dest_field

        Returns:
        DataFrame
    """
    return df.withColumn(dest_field, regexp_extract(col(src_field), regex, 1))

def drop_column(df: pyspark.sql.dataframe.DataFrame, column: str) -> pyspark.sql.dataframe.DataFrame:
    """ Drops column from DataFrame

        Keyword arguments: 
        df: DataFrame
        column: str - column to drop 

        Returns: 
        DataFrame
    """
    return df.drop(column)

def strip_column(df: pyspark.sql.dataframe.DataFrame, column: str) -> pyspark.sql.dataframe.DataFrame:
    """ Removes newline characters and strips spaces (from begin and end off string) from column
        
        Keyword arguments: 
        df: DataFrame
        column: str - column to strip 

        Returns: 
        DataFrame
    """
    df = df.withColumn(column, regexp_replace(column, '\\n', ''))
    df = df.withColumn(column, trim(col(column)))
    return df 

def split_col_by_delim(df: pyspark.sql.dataframe.DataFrame, column: str, delim: str) -> pyspark.sql.dataframe.DataFrame:
    """ Splits column value by a delimiter
        
        Keyword arguments: 
        df: DataFrame
        column: str - column to split 
        delim: delimiter to split column by

        Returns: 
        DataFrame
    """
    return df.withColumn(column, split(col(column), delim))

def typecast_strnumber_to_int(df: pyspark.sql.dataframe.DataFrame, column: str) -> pyspark.sql.dataframe.DataFrame:
    """ Removes ',' char from number and typecasts the number to int 

        Keyword Arguments:
        df: DataFrame 
        column: str - The column to run the operation on 

        Returns: 
        DataFrame
    """
    df = df.withColumn(column, regexp_replace(column, ',', ''))
    df = df.withColumn(column, df[column].cast('int'))
    return df 

def typecast_str(df: pyspark.sql.dataframe.DataFrame, column: str) -> pyspark.sql.dataframe.DataFrame: 
    """ Makes sure column is of type str

        Keyword Arguments:
        df: DataFrame
        column: str - The column t run the operation on 

        Returns DataFrame
    """
    return df.withColumn(column, df[column].cast('string'))

def typecast_strnumber_to_float(df: pyspark.sql.dataframe.DataFrame, column: str) -> pyspark.sql.dataframe.DataFrame:
    """ Removes ',' char from number and typecasts the number to float 

        Keyword Arguments:
        df: DataFrame 
        column: str - The column to run the operation on 

        Returns: 
        DataFrame
    """
    df = df.withColumn(column, regexp_replace(column, ',', ''))
    df = df.withColumn(column, df[column].cast('float'))
    return df 

def drop_cols_with_high_nullperc(df: pyspark.sql.dataframe.DataFrame, max_null_perc: int) -> pyspark.sql.dataframe.DataFrame:
    """ Takes df and drops column for which null_percentage is greater that max_null_perc
        
        Keyword arguments:
        df: DataFrame 
        max_null_perc: int between 0 and 100

        Returns: 
        DataFrame
    """
    assert max_null_perc <= 100 and max_null_perc >= 0, 'max_null_perc should be between 0 and 100'
    row_count = df.count()
    columns = df.select(df['*'])
    for column in columns:
        null_count = df.where(column.isNull()).count()
        null_percentage = null_count / row_count * 100 
        if null_percentage > max_null_perc:
            df = df.drop(column)    
    return df 
